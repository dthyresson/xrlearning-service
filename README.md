# xrlearning-service

Scripts to process xr related article data

https://api.slack.com/apps

pg_restore --verbose --clean --no-acl --no-owner -h localhost -U dthyresson -d xrinlearning-dev latest.dump

heroku buidpacks

=== xrinlearning Buildpack URLs

1. heroku/ruby
2. heroku/nodejs

# Feed config

```
# add NewEntryPrioritized webhook
curl -X "POST" "https://cloud.feedly.com/v3/enterprise/triggers" \
     -H 'Authorization: Bearer xxx' \
     -H 'Content-Type: application/json; charset=utf-8' \
     -d $'{
  "webhookURL": "https://xrinlearning-feed.netlify.app/.netlify/functions/webhooks/feedly",
  "type": "NewEntryPrioritized",
  "resourceId": "enterprise/dthyressondt/priority/global.all",
  "authorization": "Basic xxx"
}'
```
