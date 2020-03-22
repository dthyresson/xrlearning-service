#!/usr/bin/env ruby

require 'rubygems'
require 'dotenv'
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

Options = Struct.new(:channel)
options = {}

args = Options.new("world")

OptionParser.new do |opts|
  opts.banner = "Usage: Send top articles last 24 hours loist to Salck channel [options]"

  opts.on("-c", "--channel==CHANNEL", "Slack channel to post message") do |channel|
    options[:channel] = channel
    args.channel = channel
  end
end.parse!

SLACK_CHANNEL_DEFAULT = ENV.fetch('SLACK_CHANNEL_DEFAULT')
channel = options[:channel] ? (args.channel || SLACK_CHANNEL_DEFAULT) : SLACK_CHANNEL_DEFAULT

client = Slack::Web::Client.new
client.auth_test

conn = PG.connect(config)

title = "Top XR news."
blocks = [
	{
		"type": "section",
		"text": {
			"type": "mrkdwn",
			"text": title
		}
	}
]

conn.exec(%Q(
              SELECT
                row_to_json(vw_top_articles_last_24_hours) as newsletter_item
              FROM vw_top_articles_last_24_hours
              LIMIT 10
            ) ) do |result|

  logger.info "Articles to send to #{channel}: #{result.count}"

  result.each_with_index do |row, index|
    newsletter_item = JSON.parse(row.dig('newsletter_item'))

    byline = [newsletter_item['author'], newsletter_item['site']].compact.join(' - ')
    published_at = DateTime.parse(newsletter_item['published_at_with_tz'])
    article_header = ":postbox: <#{newsletter_item['url']}|#{newsletter_item['title']}> \n\t\tvia #{byline} on #{published_at.strftime('%b %e, %l:%M %p')}"

    logger.info article_header

    blocks <<
    {
    		"type": "section",
    		"text": {
    			"type": "mrkdwn",
    			"text": article_header
    		}
     }

     if newsletter_item['summary_sentences'].any?
       blocks <<
       {
       		"type": "section",
       		"text": {
       			"type": "mrkdwn",
       			"text": (newsletter_item['summary_sentences'] || []).map { |s| "* #{s}" }.join("\n")
       		}
        }
     end

     if newsletter_item['company_names'].any?
       blocks <<
       {
       		"type": "section",
       		"text": {
       			"type": "mrkdwn",
       			"text": "#{(newsletter_item['company_names'] || []).sample(10).sort.join(' - ')}"
       		}
        }
     end

     if newsletter_item['topic_labels'].any?
       blocks <<
       {
       		"type": "section",
       		"text": {
       			"type": "mrkdwn",
       			"text": "#{(newsletter_item['topic_labels'] || []).sample(10).sort.join(' - ')}"
       		}
        }
     end

     if newsletter_item['categories'].any?
       blocks <<
       {
       		"type": "section",
       		"text": {
       			"type": "mrkdwn",
       			"text": "#{(newsletter_item['categories'] || []).sample(10).sort.join(' - ')}"
       		}
        }
     end
   end
end

client.chat_postMessage(channel: channel,
                        text: title,
                        blocks: blocks,
                        as_user: true)

logger.info "Slack message send to #{channel}"
