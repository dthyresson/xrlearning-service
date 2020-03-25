#!/usr/bin/env ruby

require 'rubygems'
require 'dotenv'
require 'httparty'
require 'pg'
require 'logger'

def logger
  @logger ||= Logger.new(STDOUT)
end

Dotenv.load

logger.info 'Textrazor loader ...'

config = {
  host: ENV['PGHOST'],
  port: ENV['PGPORT'],
  dbname: ENV['PGDATABASE'],
  user: ENV['PGUSER'],
  password: ENV['PGPASSWORD'],
}

conn = PG.connect(config)

def textrazor_analyze(url:)
  response = HTTParty.post('https://api.textrazor.com/', {
                        timeout: 10000,
                        headers: {
                                    'x-textrazor-key' => ENV['TEXT_RAZOR_API_TOKEN'],
                                    'Content-Type' => 'application/x-www-form-urlencoded; charset=utf-8',
                                   },
                         multipart: true,
                         body: {
                           url: url,
                           extractors: 'entities,topics,phrases',
                           'cleanup.returnCleaned' => true,
                           'cleanup.mode' => 'cleanHTML',
                           classifiers: 'textrazor_mediatopics',
                         }
                      })

  if response['error']
   logger.info "Error >>> #{response['error']}"
   nil
  else
    if response['response']
      response['response']
    else
      nil
    end
  end
end

refresh = false

conn.exec(%Q(
              SELECT
                e.feedly_id
              , d.url
              FROM feedly_entries e
              LEFT JOIN feedly_entry_text_analyses a on e.feedly_id = a.feedly_id
              JOIN vw_feedly_entry_details d on d.feedly_id = e.feedly_id
              WHERE a.feedly_id IS NULL
              ORDER BY e.created_at, e.feedly_id
            ) ) do |result|
  logger.info "Entries to enrich: #{result.count}"
  result.each_with_index do |row, index|
    refresh = true
    feedly_id = row['feedly_id']
    url = row['url']

    logger.info "Analyzing #{index} -> #{feedly_id} #{url}"

    if payload = textrazor_analyze(url: url)
      stm = %Q(INSERT INTO feedly_entry_text_analyses (created_at,
                                                       updated_at,
                                                       feedly_id,
                                                       payload,
                                                       status_code,
                                                       status_at,
                                                       parsed_at)
               VALUES (
                 clock_timestamp(),
                 clock_timestamp(),
                 $1,
                 $2,
                 200,
                 clock_timestamp(),
                 clock_timestamp()
               )
             ON CONFLICT (feedly_id)
             DO UPDATE SET (payload,
                            updated_at,
                            status_code,
                            status_at,
                            parsed_at) = ($2,
                                          clock_timestamp(),
                                          200,
                                          clock_timestamp(),
                                          clock_timestamp()
                                          )
             WHERE feedly_entry_text_analyses.feedly_id = $1)

       conn.exec_params(stm, [feedly_id, payload.to_json])

      if ((index + 1) % 5) == 0
        logger.info ('sleeping ...')
        sleep(5)
      end
    else
      stm = %Q(INSERT INTO feedly_entry_text_analyses (created_at,
                                                       updated_at,
                                                       feedly_id,
                                                       payload,
                                                       status_code,
                                                       status_at,
                                                       parsed_at)
               VALUES (
                 clock_timestamp(),
                 clock_timestamp(),
                 $1,
                 null,
                 404,
                 clock_timestamp(),
                 null
               )
             ON CONFLICT (feedly_id)
             DO UPDATE SET (updated_at,
                            status_code,
                            status_at,
                            parsed_at) = (clock_timestamp(),
                                          400,
                                          clock_timestamp(),
                                          null
                                          )
             WHERE feedly_entry_text_analyses.feedly_id = $1)

      conn.exec_params(stm, [feedly_id])
      logger.info ">>> Issue with #{feedly_id} #{url}"
    end
  end
end

if refresh
  logger.info 'Refreshing views ...'
  conn.exec('REFRESH MATERIALIZED VIEW CONCURRENTLY articles;')
  conn.exec('REFRESH MATERIALIZED VIEW CONCURRENTLY xr_company_articles_with_sectors;')
  # conn.exec('REFRESH MATERIALIZED VIEW CONCURRENTLY vw_xr_channel_articles;')
  # conn.exec('REFRESH MATERIALIZED VIEW CONCURRENTLY vw_xr_channel_article_details;')
  logger.info '... done!'
end
conn.finish
