#!/usr/bin/env ruby

require 'rubygems'
require 'dotenv'
require 'httparty'
require 'pg'
require 'active_support/core_ext/numeric/time'
require 'active_support/core_ext/array'
require 'logger'
require 'optparse'

Dotenv.load


def logger
  @logger ||= Logger.new(STDOUT)
end

config = {
  host: ENV['PGHOST'],
  port: ENV['PGPORT'],
  dbname: ENV['PGDATABASE'],
  user: ENV['PGUSER'],
  password: ENV['PGPASSWORD'],
}

Options = Struct.new(:since)
options = {}

args = Options.new("world")

OptionParser.new do |opts|
  opts.banner = "Usage: get_feedly_priority_stream.rb [options]"

  opts.on("-s", "--since=DAYS", "Get the enterprise/dthyressondt/priority/global.all stream") do |s|
    options[:since] = s
    args.since = s
  end
end.parse!

logger.info 'Getting Feedly priority stream ...'


conn = PG.connect(config)


def get_feedly_entries
  begin

    HTTParty.get('https://cloud.feedly.com/v3/streams/contents', {
                        timeout: 60000,
                        # debug_output: logger,
                        headers: {
                                    'Authorization' => "Bearer #{ENV['FEEDLY_API_TOKEN']}",
                                    'Content-Type' => 'application/json',
                                   },
                         query: {
                                  streamId: 'enterprise/dthyressondt/priority/global.all',
                                  count: 1000,
                                  unreadOnly: false
                                },
                      })['items']
  rescue
    []
  end
end

def upsert_feedly_entry(conn:, feedly_entry:)
  # logger.debug feedly_entry
  id = feedly_entry.dig('id')
  published_at = feedly_entry.dig('published') / 1000;

  # logger.info [id, feedly_entry, published_at]

  conn.exec(%Q(INSERT INTO feedly_entries (created_at, updated_at, feedly_id, payload, published_at)
                            VALUES (
                              clock_timestamp(),
                              clock_timestamp(),
                              $1,
                              $2,
                              TIMESTAMP WITHOUT TIME ZONE 'epoch' + $3 * INTERVAL '1 second'
                            )
                          ON CONFLICT (feedly_id)
                          DO UPDATE SET (payload, published_at, updated_at) = ($2,
                                                                               TIMESTAMP WITHOUT TIME ZONE 'epoch' + $3 * INTERVAL '1 second',
                                                                               clock_timestamp())
                          WHERE feedly_entries.feedly_id = $1),
                        [id, feedly_entry.to_json, published_at])
end


# Maybe refresh 1 day, 2 days, 4 days and 7 days after the crawl date?
#  The engagement value changes the most in the first 2 days, after that it tapers off quickly.

since = options[:since] ? (args.since || 2).to_i : 1

logger.info "Refreshing Feedly priority stream since #{since} days ago/#{since.days.ago} ... "

# update_feedly_engagement(conn: conn, since: since.days.ago)

get_feedly_entries.each do |feedly_entry|
  # logger.info feedly_entry
  upsert_feedly_entry(conn: conn, feedly_entry: feedly_entry)
end
