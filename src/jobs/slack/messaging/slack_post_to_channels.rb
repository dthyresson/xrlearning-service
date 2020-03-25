#!/usr/bin/env ruby

require 'rubygems'
require 'dotenv'
require 'imgix'
require 'logger'
# require 'optparse'
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

# Options = Struct.new(:channel)
# options = {}
#
# args = Options.new("world")
#
# OptionParser.new do |opts|
#   opts.banner = "Usage: Send articles to Slack channels [options]"
#
#   opts.on("-c", "--channel==CHANNEL", "Slack channel to post message") do |channel|
#     options[:channel] = channel
#     args.channel = channel
#   end
# end.parse!

IMGIX = Imgix::Client.new(host: ENV.fetch('IMGIX_HOST'), secure_url_token: ENV.fetch('IMGIX_SECRET'))

# SLACK_CHANNEL_DEFAULT = ENV.fetch('SLACK_CHANNEL_DEFAULT')
# channel = options[:channel] ? (args.channel || SLACK_CHANNEL_DEFAULT) : SLACK_CHANNEL_DEFAULT

client = Slack::Web::Client.new
client.auth_test

conn = PG.connect(config)
#
# logger.info 'Refreshing articles ...'
# conn.exec('REFRESH MATERIALIZED VIEW CONCURRENTLY vw_xr_channel_articles;')
# conn.exec('REFRESH MATERIALIZED VIEW CONCURRENTLY vw_xr_channel_article_details;')
# logger.info '... articles refreshed!'
#
conn.exec(%Q(
              SELECT
                row_to_json(vw_xr_channel_articles_unsent_by_channel_and_target) as newsletter_item
              FROM vw_xr_channel_articles_unsent_by_channel_and_target
              where target = 'slack'
            ) ) do |result|

  logger.info "To send: #{result.count}"


  result.each_with_index do |row, index|

    blocks = []
    company_list = []

    newsletter_item = JSON.parse(row.dig('newsletter_item'))

    channel_id = newsletter_item['channel_id']
    channel = newsletter_item['target_id']

    logger.info "Article #{newsletter_item['title']} to send to #{channel}"

    title = newsletter_item['title']

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

     if newsletter_item['summary_sentences'].present? && newsletter_item['summary_sentences'].any?
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

     if newsletter_item['company_names'].present? && newsletter_item['company_names'].any?
       company_list += newsletter_item['company_names']
       captions << "*Companies and Organizations:* #{(newsletter_item['company_names'] || []).compact.sort.join(' - ')}"
     end

     if newsletter_item['topic_labels'].present? && newsletter_item['topic_labels'].any?
      captions << "*XR Topics:* #{(newsletter_item['topic_labels'] || []).compact.sort.join(' - ')}"
     end

     if newsletter_item['categories'].present? && newsletter_item['categories'].any?
       captions << "*Market:* #{(newsletter_item['categories'] || []).compact.sort.join(' - ')}"
     end

     if captions.present? && captions.any?
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

      message_title = "#{title} mention #{company_list.flatten.uniq.compact.sort.join(', ')}"
      blocks = blocks.unshift(
       {
         "type": "section",
         "text": {
           "type": "mrkdwn",
           "text": message_title
         }
       }
      )

      published_at = DateTime.parse(newsletter_item['published_at'])
      mins = published_at.hour
      secs = published_at.sec + rand(60)

      post_at = Time.current.utc + mins.minutes + secs.seconds

      message = {
                  channel: channel_id,
                  text: message_title,
                  blocks: blocks.to_json,
                  as_user: true,
                  post_at: post_at.to_i
                }

      logger.info message.to_json

      client.chat_scheduleMessage(message)

      conn.exec("update channels set last_sent_at = $1 where id = $2", [newsletter_item['created_at'], channel_id])

      logger.info "Slack message sending to #{channel} as #{post_at}"
   end
end
