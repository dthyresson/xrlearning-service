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

Options = Struct.new(:channel)
options = {}

args = Options.new("world")

OptionParser.new do |opts|
  opts.banner = "Usage: Send top articles last 24 hours to Slack channel [options]"

  opts.on("-c", "--channel==CHANNEL", "Slack channel to post message") do |channel|
    options[:channel] = channel
    args.channel = channel
  end
end.parse!

IMGIX = Imgix::Client.new(host: ENV.fetch('IMGIX_HOST'), secure_url_token: ENV.fetch('IMGIX_SECRET'))

SLACK_CHANNEL_DEFAULT = ENV.fetch('SLACK_CHANNEL_DEFAULT')
channel = options[:channel] ? (args.channel || SLACK_CHANNEL_DEFAULT) : SLACK_CHANNEL_DEFAULT

client = Slack::Web::Client.new
client.auth_test

conn = PG.connect(config)

title = ":top: *Top XR news*"
blocks = []
company_list = []

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
    article_header = ":postbox: <#{newsletter_item['url']}|#{newsletter_item['title']}>"

    logger.info article_header



     blocks <<
       {
       		"type": "section",
       		"text": {
       			"type": "mrkdwn",
       			"text": article_header
       		}
        }

      if newsletter_item['image_url']
        blocks <<
          {
              type: "image",
              title: {
                type: "plain_text",
                text: newsletter_item['site'],
                emoji: true
              },
              image_url: IMGIX.path(newsletter_item['image_url']).to_url(h: 180),
              alt_text: newsletter_item['site']
            }
      end

     if newsletter_item['summary_sentences'].any?
       blocks <<
       {
       		"type": "section",
       		"text": {
       			"type": "mrkdwn",
       			"text": (newsletter_item['summary_sentences'] || []).map { |s| "* #{s}" }.join("\n\n")
       		}
        }
     end

     captions = []

     if newsletter_item['company_names'].any?
       company_list += newsletter_item['company_names']
       captions << "*Companies and Organizations:* #{(newsletter_item['company_names'] || []).sample(10).sort.join(' - ')}"
     end

     if newsletter_item['topic_labels'].any?
      captions << "*XR Topics:* #{(newsletter_item['topic_labels'] || []).sample(10).sort.join(' - ')}"
     end

     if newsletter_item['categories'].any?
       captions << "*Market:* #{(newsletter_item['categories'] || []).sample(10).sort.join(' - ')}"
     end

     if captions.any?
       elements = captions.map do |caption|
         {
           "type": "mrkdwn",
           "text": caption
         }
       end

       blocks <<
        {
    			"type": "context",
    			"elements": elements
    		}
     end

     blocks <<
      {
  			"type": "context",
  			"elements": [
  				{
  					"type": "mrkdwn",
  					"text": "*Author:* #{byline} on #{published_at.strftime('%b %e, %l:%M %p')}"
  				}
  			]
  		}
   end
end

message_title = "#{title} discusses #{company_list.flatten.uniq.sort.join(', ')}"
blocks = blocks.unshift(
	{
		"type": "section",
		"text": {
			"type": "mrkdwn",
			"text": message_title
		}
	}
)

client.chat_postMessage(channel: channel,
                        text: message_title,
                        blocks: blocks.take(50),
                        as_user: true)

logger.info "Slack message send to #{channel}"
