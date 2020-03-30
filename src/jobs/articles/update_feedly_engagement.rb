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
  opts.banner = "Usage: update_feedly_engagement.rb [options]"

  opts.on("-s", "--since==DAYS", "Update feedly entry engagement crawled since days ago") do |s|
    options[:since] = s
    args.since = s
  end
end.parse!

logger.info 'Update Feedly Engagement ...'


conn = PG.connect(config)


def get_feedly_entries(feedly_ids:)
  begin
    HTTParty.post('https://cloud.feedly.com/v3/entries/.mget', {
                        timeout: 60000,
                        headers: {
                                    'Authorization' => "Bearer #{ENV['FEEDLY_API_TOKEN']}",
                                    'Content-Type' => 'application/json',
                                   },
                         body: { id: feedly_ids }.to_json,
                      })
  rescue
    []
  end
end

def update_feedly_entry(conn:, feedly_entry:)
  id = feedly_entry.dig('id')
  conn.exec(%Q(UPDATE feedly_entries
                 SET updated_at = clock_timestamp(),
                     payload = $2
                WHERE feedly_id = $1),[id, feedly_entry.to_json])

end


def update_feedly_engagement(conn:, since: 7.days.ago)
  conn.exec(%Q(
                SELECT
                  e.feedly_id
                FROM vw_feedly_entry_details e
                JOIN vw_feedly_entry_details d on d.feedly_id = e.feedly_id
                WHERE e.crawled_at >= $1
                ORDER BY e.created_at, e.feedly_id
              ), [since.beginning_of_day]) do |result|
    puts "Entries to enrich: #{result.count}"
    result.column_values(0).in_groups_of(250) do |group|
      feedly_ids = group.compact
      get_feedly_entries(feedly_ids: feedly_ids).each do |feedly_entry|
        update_feedly_entry(conn: conn, feedly_entry: feedly_entry)
      end
      sleep(2)
      logger.info 'next'
    end
  end
end

# Maybe refresh 1 day, 2 days, 4 days and 7 days after the crawl date?
#  The engagement value changes the most in the first 2 days, after that it tapers off quickly.

since = options[:since] ? (args.since || 2).to_i : 1

logger.info "Refreshing feedly engagement since #{since} days ago/#{since.days.ago} ... "

update_feedly_engagement(conn: conn, since: since.days.ago)
