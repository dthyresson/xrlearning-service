#!/usr/bin/env ruby

require 'rubygems'
require 'dotenv'
require 'imgix'
require 'logger'
require 'optparse'
require 'pg'
require 'slack-ruby-client'

def logger
  @logger ||= Logger.new(STDOUT)
end

Dotenv.load

config = {
  host: ENV['PGHOST'],
  port: ENV['PGPORT'],
  dbname: ENV['PGDATABASE'],
  user: ENV['PGUSER'],
  password: ENV['PGPASSWORD'],
}

Slack.configure do |config|
  config.token = ENV.fetch('SLACK_API_TOKEN')
end

Options = Struct.new(:channel, :period)
options = {}

args = Options.new("world")

OptionParser.new do |opts|
  opts.banner = "Usage: Send top xr companies to Slack channel [options]"

  opts.on("-p", "--period==PERIOD", "Time period of day, week or month.") do |period|
    options[:period] = period
    args.period = period
  end

  opts.on("-c", "--channel==CHANNEL", "Slack channel to post message") do |channel|
    options[:channel] = channel
    args.channel = channel
  end
end.parse!

SLACK_CHANNEL_DEFAULT = ENV.fetch('SLACK_CHANNEL_DEFAULT')
channel = options[:channel] ? (args.channel || SLACK_CHANNEL_DEFAULT) : SLACK_CHANNEL_DEFAULT

period = options[:period] ? (args.period || 'week') : 'week'

Time.zone = "Eastern Time (US & Canada)"

time_period = case period
  when 'day'
    DateTime.current.beginning_of_day()
  when 'yesterday'
    DateTime.current.beginning_of_day() - 1.day
  when 'week'
    DateTime.current.beginning_of_week()
  when 'last-week'
    DateTime.current.beginning_of_week() - 7.days
  when 'month'
    DateTime.current.beginning_of_month()
  when 'last-month'
    DateTime.current.beginning_of_month() - 1.month
  end

time_period_label = case period
when 'day'
  period
when 'yesterday'
  'day'
when 'week'
  period
when 'last-week'
  'week'
when 'month'
  period
when 'last-month'
  'month'
end

client = Slack::Web::Client.new
client.auth_test

IMGIX = Imgix::Client.new(host: ENV.fetch('IMGIX_HOST'), secure_url_token: ENV.fetch('IMGIX_SECRET'))

conn = PG.connect(config)

blocks = []
fields = []

conn.exec(%Q(
              SELECT
                row_to_json(vw_xr_ranked_companies_by_#{time_period_label}) as rankings
              FROM vw_xr_ranked_companies_by_#{time_period_label}
              WHERE time_period = $1
              ORDER BY rank
              LIMIT 10
            ), [time_period] ) do |result|

  logger.info "Rankings to send to #{channel}: #{result.count}"

  result.each_with_index do |row, index|
    rankings = JSON.parse(row.dig('rankings'))
    fields << {
                "type": "mrkdwn",
                "text": "#{rankings['rank'].to_s.rjust(2)} | *#{rankings['name']}* (#{rankings['freq']})",
              }
  end
end

blocks << {
  "type": "section",
  "fields": fields
}

title = ":100: *Top Mentioned Companies in XR"

message_title = "#{title} by #{time_period_label.titleize}*"

blocks <<
    {
			"type": "context",
			"elements": [
				{
					"type": "mrkdwn",
					"text": "*#{time_period_label.titleize}*: #{time_period.strftime('%b %d, %Y')}"
				},
			]
    }

blocks.unshift(
  	{
  		"type": "section",
  		"text": {
  			"type": "mrkdwn",
  			"text": message_title
  		}
  	}
)

logger.info message_title
logger.info blocks

client.chat_postMessage(channel: channel,
                        text: message_title,
                        blocks: blocks.take(50),
                        as_user: true)

logger.info "Slack message send to #{channel}"
