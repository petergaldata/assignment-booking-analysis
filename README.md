# README - Spark SQL Batch Job for Booking Analysis

## Overview

This batch job, written in PySpark, performs an analysis on airline booking data. The script processes booking and airport information, executes a predefined SQL query for analysis, and saves the results in CSV format. This README outlines the necessary steps to configure and run the job successfully.

## Prerequisites

To run this batch job, you will need:

- **Apache Spark**: Ensure that Apache Spark is installed and properly configured on your system. The script is compatible with the version of Spark that supports PySpark.
- **Python**: Python 3.x should be installed.
- **Python Libraries**: The `pyspark`  library is required. Install them using pip if not already available.
- **Input Data Files**: You need two data files:
  - Bookings data in JSON format.
  - Airports data in CSV format.
- **SQL Query File**: An SQL file (`booking_analysis.sql`) containing the query for analysis.

## Setting Up Apache Spark in Docker and Using VS Code

**Prerequisites**
- Docker Desktop installed
- Visual Studio Code installed
- Basic knowledge of Docker and VS Code

### Step 1: Pulling the Spark Docker Image

1. **Open a Terminal or Command Prompt**.
2. **Pull the Apache Spark Docker image** by running:

   ```bash
   docker pull bitnami/spark
   ```

   This command downloads the official Bitnami Spark image.

### Step 2: Running the Spark Docker Container

1. **Run the Spark container** using:

   ```bash
   docker run -d -p 4040:4040 -p 8080:8080 --name spark bitnami/spark
   ```

   This command starts a Spark container and maps the necessary ports (4040 for Spark jobs UI, 8080 for Spark master UI).

### Step 3: Setting Up VS Code

1. **Install the Remote - Containers Extension in VS Code**:
   - Open VS Code.
   - Go to Extensions (sidebar).
   - Search for "Remote - Containers" and install it.

2. **Attach to the Running Spark Container**:
   - Open the Command Palette (Ctrl+Shift+P or Cmd+Shift+P on Mac).
   - Type "Remote-Containers: Attach to Running Container...".
   - Select the running Spark container.

## Configuration

Configure the batch job by editing the `config.json` file. This file should include the following fields:

- `bookings_location`: The file path to the bookings JSON data.
- `airports_location`: The file path to the airports CSV data.
- `start_date` and `end_date`: The date range for the analysis. Format these dates as `YYYY-MM-DD`. If unspecified, the script will analyze the full date range in the data.

Example `config.json`:

```json
{
  "bookings_location": "/path/to/bookings.json",
  "airports_location": "/path/to/airports.csv",
  "start_date": "2022-01-01",
  "end_date": "2022-12-31"
}
```

## Running the Batch Job

To run the job:

1. **Prepare Data Files**: Ensure that the bookings and airports data files are in the specified locations in your `config.json`.
2. **Configure `config.json`**: Check that `config.json` points to the correct file paths and includes the desired date range for analysis.
3. **Execute the Script**: Run the script in a Spark environment. This is typically done with the command:

   ```shell
   spark-submit booking_analysis.py
   ```

## Output
The script writes the analysis results to CSV files in the directory booking_analysis/YYYY_MM_DD/, where YYYY_MM_DD represents the current date. The results are partitioned across multiple CSV files in this directory.

## Output

The script writes the analysis results to CSV files in the directory `booking_analysis/YYYY_MM_DD/`, where `YYYY_MM_DD` represents the current date. The results are partitioned across multiple CSV files in this directory.

## Additional Notes

- **Data Format**: The script expects bookings data in a specific JSON structure and airports data in a CSV format. Ensure that your data files match these expected formats.
- **Resource Considerations**: This job is designed for distributed processing with Spark. Ensure that your Spark cluster has sufficient resources to handle the size of your dataset.
- **Error Handling**: The script includes basic error handling for file reading and JSON parsing. Modify or extend error handling as needed for your specific use case.

## Support

For any issues or questions regarding this batch job, please refer to the project repository or contact the maintainers.


