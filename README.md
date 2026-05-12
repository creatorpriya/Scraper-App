# Scraper_System

Scraper_System is a scalable web scraping and data processing platform built with Node.js. It consumes scraping jobs from AWS SQS queues, fetches webpage content, extracts structured company and review data using custom scraper modules, and stores processed results into MongoDB collections.

The system supports company intelligence extraction, review monitoring, SOS business searches, and multi-source data collection with automated retries, logging, and session management.

## Features

* AWS SQS-based distributed job processing
* Automated webpage HTML fetching
* Multi-source company data scraping
* Review extraction & normalization
* SOS business entity scraping
* MongoDB bulk data storage
* Session-based API authentication
* Concurrent message processing
* Retry & failover handling
* Logging & monitoring system
* HTML debug file generation
* Dynamic scraper configuration support

## Tech Stack

* Node.js
* Axios
* MongoDB
* AWS SQS
* AWS SDK
* JSON Processing

## Supported Scraper Types

* Company Data Scrapers
* Review Scrapers
* SOS/Business Entity Scrapers
* BuiltWith Scrapers
* Yahoo Finance Scrapers
* Crunchbase Scrapers
* PitchBook Scrapers
* Owler Scrapers

## Workflow

1. Consume scraping jobs from AWS SQS
2. Authenticate and generate session tokens
3. Fetch webpage HTML content
4. Process data using configured scraper extractors
5. Normalize and validate extracted data
6. Store results in MongoDB
7. Generate logs and processing reports
8. Delete processed jobs from SQS

## Data Processing Capabilities

* Review sentiment & rating normalization
* Date parsing & validation
* Duplicate-safe MongoDB upserts
* Incident generation for poor reviews
* HTML fallback extraction support
* API + scraper hybrid extraction model

## Scalability

The system supports:

* Concurrent queue processing
* Distributed scraping pipelines
* Retry-safe execution
* Automatic session refresh
* Large-scale data extraction
* Fault-tolerant scraping workflows

Ideal for company intelligence platforms, review aggregation systems, compliance monitoring, business entity extraction, and large-scale web scraping pipelines.
