# Simple Elasticsearch 6.x API for Golang

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Build Status](https://travis-ci.org/NextronSystems/go-elasticsearch.svg?branch=master)](https://travis-ci.org/NextronSystems/go-elasticsearch)
[![Go Report Card](https://goreportcard.com/badge/github.com/NextronSystems/go-elasticsearch)](https://goreportcard.com/report/github.com/NextronSystems/go-elasticsearch)
[![GoDoc](https://godoc.org/github.com/NextronSystems/go-elasticsearch?status.svg)](https://godoc.org/github.com/NextronSystems/go-elasticsearch)

## Features

- Document 
  - Insert document
  - Update document
  - Delete document
  - Get document
  
- Query
  - Update documents by query
  - Delete documents by query
  - Get documents by query (paging)
  - Get documents by query (scroll)
  
- Bulk
  - Insert documents
  
- Index
  - Delete index
  - Refresh index 
  - Add Template
  - Delete Template
  
- Aggregate
  - Term Aggregate (Get most frequent values of a field) [Terms Aggregation](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html)
  - Range Aggregate (Get min- and max-value of a field) [Range Aggregation](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-range-aggregation.html)
  - Cardinality Aggregate (Get unique count of a field) [Cardinality Aggregation](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-cardinality-aggregation.html)
  - Composite Term Aggregate (Get all values of a field) [Composite Aggregation](https://www.elastic.co/guide/en/elasticsearch/reference/master/search-aggregations-bucket-composite-aggregation.html)
  
- Other
  - Connection test
  - Health status [Cluster Health](https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-health.html)
  - Optional debug logs

## Tested with Elasticsearch 6.1.1
