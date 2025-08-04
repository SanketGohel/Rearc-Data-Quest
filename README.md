# Rearc Data Quest - BLS Data Processing Pipeline

A serverless data processing pipeline built with AWS SAM that scrapes Bureau of Labor Statistics (BLS) data and population data, then generates analytical reports.

## 🏗️ Architecture

This project implements a serverless architecture using AWS SAM with the following components:

- **S3 Bucket**: Stores scraped BLS data and population data
- **Lambda Function 1 (scrapingData)**: Scrapes BLS data and population data from external APIs
- **Lambda Function 2 (reportGeneration)**: Processes data and generates analytical reports
- **SQS Queue**: Triggers report generation when new data is uploaded
- **CloudWatch Logs**: Provides monitoring and debugging capabilities

## 📊 Data Sources

### BLS Data (Part 1)
- **Source**: https://download.bls.gov/pub/time.series/pr/
- **Storage**: S3 bucket with prefix `part1/`
- **Format**: Tab-delimited files with `.Current` extension
- **Content**: Employment and labor statistics time series data

### Population Data (Part 2)
- **Source**: DataUSA API (Honolulu)
- **Storage**: S3 bucket with prefix `part2/`
- **Format**: JSON files with timestamps
- **Content**: US population data by year

## 🚀 Features

### Data Collection
- **Automated Scraping**: Fetches current BLS data files and compares with existing S3 objects
- **Incremental Updates**: Only downloads new or changed files
- **Cleanup**: Removes obsolete files from S3
- **Population Data**: Fetches current population statistics from DataUSA API

### Data Analysis
- **Statistical Analysis**: Calculates mean and standard deviation of US population (2013-2018)
- **Time Series Analysis**: Identifies best performing years for each data series
- **Data Integration**: Merges BLS time series data with population data
- **Data Cleaning**: Strips whitespace and normalizes data types

## 🛠️ Prerequisites

- AWS CLI configured with appropriate permissions
- Python 3.9+ (for local development)
- AWS SAM CLI

## 📦 Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd RearcProject
   ```

2. **Install dependencies**
   ```bash
   # For Function 1 (Data Scraping)
   pip install -r src/Function/requirements.txt
   
   # For Function 2 (Report Generation)
   pip install -r src/Function2/requirements.txt
   ```

3. **Deploy to AWS**
   ```bash
   sam build
   sam deploy --guided
   ```

## 🔧 Configuration

The project uses the following environment variables:

- `BLSDATA_BUCKET_NAME`: S3 bucket name for data storage
- `BLSDATA_BUCKET_ARN`: S3 bucket ARN
- `SQS_QUEUE_URL`: SQS queue URL for triggering report generation

## 📈 Usage

### Manual Data Collection
The `scrapingData` function can be invoked manually to collect data:

```bash
aws lambda invoke --function-name scrapingData response.json
```

### Automatic Report Generation
Reports are automatically generated when new population data is uploaded to S3 (triggered via SQS).

### Generated Reports

1. **Population Statistics (2013-2018)**
   - Mean population
   - Standard deviation

2. **Best Year Analysis**
   - For each series_id, identifies the year with the highest summed values
   - Returns: series_id, best_year, max_value

3. **Integrated Data Report**
   - Merges BLS time series data with population data
   - Filters by specific series_id and period (default: PRS30006032, Q01)

## 🔍 Monitoring

- **CloudWatch Logs**: Each Lambda function has dedicated log groups
- **S3 Notifications**: Automatic triggering of report generation
- **Error Handling**: Comprehensive error logging and exception handling

## 🏗️ Project Structure

```
RearcProject/
├── template.yaml          # SAM template with AWS resources
├── samconfig.toml        # SAM deployment configuration
├── src/
│   ├── Function/         # Data scraping Lambda
│   │   ├── handler.py    # Main scraping logic
│   │   └── requirements.txt
│   └── Function2/        # Report generation Lambda
│       ├── handler.py    # Data analysis and reporting
│       └── requirements.txt
└── README.md
```

## 🔐 Security

- **S3 Encryption**: Server-side encryption with AWS KMS
- **IAM Roles**: Least privilege access for Lambda functions
- **Public Access**: S3 bucket blocks public access
- **Transport Security**: Enforces HTTPS for S3 operations

## 🧪 Testing

### Local Testing
```bash
# Test data scraping function
sam local invoke scrapingData --event events/scraping-event.json

# Test report generation function
sam local invoke reportGeneration --event events/report-event.json
```

### Integration Testing
1. Deploy the stack
2. Manually invoke the scraping function
3. Verify S3 objects are created
4. Check that report generation is triggered automatically

## 📝 API Endpoints

### Data Sources
- **BLS Data**: https://download.bls.gov/pub/time.series/pr/
- **Population Data**: https://honolulu-api.datausa.io/tesseract/data.jsonrecords

### Headers
- User-Agent: EmploymentDataFetcher/1.0 (sanketgohelt1992@gmail.com)
- Content-Type: text/html

## 🚨 Troubleshooting

### Common Issues

1. **S3 Access Denied**
   - Verify IAM roles have appropriate S3 permissions
   - Check bucket policy and encryption settings

2. **Lambda Timeout**
   - Increase timeout in template.yaml (currently 900s for scraping, 30s for reports)
   - Monitor memory usage and adjust if needed

3. **SQS Message Processing**
   - Check CloudWatch logs for Lambda execution errors
   - Verify SQS queue permissions

### Debugging

- Enable X-Ray tracing for detailed request tracking
- Check CloudWatch logs for each Lambda function
- Monitor S3 bucket for file uploads and deletions

## 📄 License

This project is part of the Rearc Data Quest challenge.

## 👥 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📞 Support

For issues and questions, please contact: sanketgohelt1992@gmail.com

---

**Stack Name**: rearc-data-quest  
**Region**: us-east-1  
**Deployment**: AWS SAM 