![CI](https://github.com/chin-tech/FurAngelInvoiceParser/actions/workflows/ci.yml/badge.svg)
# FurAngelInvoiceParser

## Project Overview

FurAngelInvoiceParser is a Python application designed to automate the extraction of invoice data for Fur Angel, specifically tailored for deployment and operation within a Google Cloud Platform (GCP) environment. It processes PDF invoices retrieved from a designated Gmail inbox or Google Drive folder, extracts key information such as invoice number, date, vendor details, and line items, and then stores this structured data into an ASM (Animal Shelter Manager) database. This automation significantly reduces manual data entry, improves accuracy, and streamlines accounting processes for Fur Angel by leveraging Google Cloud services.

## Features

  * **PDF Text Extraction:** Reads and extracts text content from PDF invoice documents.
  * **Gmail & Google Drive Integration:** Automatically fetches new invoice PDFs from a specified Gmail label/folder or Google Drive subfolder.
  * **Data Parsing:** Intelligently identifies and extracts relevant fields like invoice number, date, vendor, and itemized lists.
  * **ASM Database Storage:** Persists the parsed and structured invoice data into an ASM database for centralized storage and easy access.
  * **Error Handling:** Includes mechanisms to handle common parsing errors and flag unidentifiable data for manual review.

-----

## Getting Started

### Prerequisites

  * Python 3.x
  * `uv` (Recommended, but pip could work)
  * A Google Cloud Platform (GCP) project with billing enabled.
  * Enabled Google Cloud APIs:
      * Gmail API
      * Google Drive API
  * Service Account with appropriate permissions for:
      * Reading emails from a specified Gmail inbox (e.g., `invoice@furangel.com`) with access to a subfolder (e.g., `Inbox/Invoices`).
      * Reading files from a specific Google Drive folder.
      * Writing data to the ASM database.

### Installation

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/chin-tech/FurAngelInvoiceParser.git
    cd FurAngelInvoiceParser
    ```

2.  **Install dependencies:**

    ```bash
    # Locally
    uv sync
    # Remotely, just use the Dockerfile
    ```

3.  **GCP Setup:**

      * **Create a Service Account:** In your GCP project, navigate to IAM & Admin -\> Service Accounts and create a new service account. Download the JSON key file.
      * **Grant Permissions:** Grant the necessary roles to this service account:
          * **Gmail:** Ensure the service account has delegated domain-wide authority or is explicitly granted access to read emails from the specified email address and folder (e.g., `invoice@furangel.com`, `Inbox/Invoices`). This typically involves setting up domain-wide delegation in your Google Workspace admin console.
          * **Google Drive:** Grant `Google Drive Reader` or `Google Drive Editor` role to the service account on the specific Google Drive folder where invoices are stored.
          * **ASM Database:** Have an appropriate roled account for your ASM database  to allow data insertion.
      * **Store Service Account Key:** Place the downloaded service account JSON key file in a secure location within your project, for example, `credentials/service-account-key.json`. **Do not commit this file to your public repository.**
      * **Configure Environment Variables:** Set environment variables for the path to your service account key and any other sensitive configurations (e.g., database connection strings, email address to monitor, Google Drive folder ID).

### Usage

1.  **Ensure GCP Setup is Complete:** Verify your service account, API keys, and environment variables are correctly configured.

2.  **Run the application (Locally for testing):**

    ```bash
    python src/main.py
    ```

3.  **Deployment to GCP (e.g., Cloud Functions, Cloud Run, or App Engine):**
    Refer to Google Cloud documentation for deploying Python applications to your chosen service. You will need to configure the execution environment with the necessary environment variables and service account.

    *Example (Cloud Functions):*

    ```bash
    gcloud functions deploy fur-angel-invoice-parser \
      --runtime python39 \
      --entry-point main \
      --trigger-topic invoice_trigger \
      --service-account [YOUR_SERVICE_ACCOUNT_EMAIL] \
      --set-env-vars GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials/service-account-key.json,GMAIL_EMAIL=invoice@furangel.com,DRIVE_FOLDER_ID=your_drive_folder_id,ASM_DB_CONNECTION_STRING=your_db_connection_string
    ```

    *(Note: The above is a simplified example. Actual deployment will vary based on your specific GCP service and architecture.)*

-----
## Contributing

We welcome contributions to improve FurAngelInvoiceParser\! If you'd like to contribute, please follow these steps:

1.  Fork the repository.
2.  Create a new branch (`git checkout -b feature/your-feature-name`).
3.  Make your changes.
4.  Commit your changes (`git commit -m 'Add some feature'`).
5.  Push to the branch (`git push origin feature/your-feature-name`).
6.  Open a Pull Request.

-----

## License

This project is licensed under the GNU Public License.
