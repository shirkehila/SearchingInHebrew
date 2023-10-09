# Use the official Elasticsearch image as the base image
FROM docker.elastic.co/elasticsearch/elasticsearch:7.16.0

# Install the ICU Analysis plugin
RUN elasticsearch-plugin install analysis-icu
ENV PLUGIN_URL=https://d.bigdataboutique.com/2ae415f36a5efcde1a34060e8b796d56goe7yh9ne24/elasticsearch-analysis-hebrew/analysis-hebrew-7.16.0-commercial.zip
RUN  yes | /usr/share/elasticsearch/bin/elasticsearch-plugin install $PLUGIN_URL
