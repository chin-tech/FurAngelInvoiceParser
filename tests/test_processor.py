import pytest
import io
import pandas as pd
from datetime import datetime as dt, timedelta as td
from unittest.mock import patch, Mock, ANY
from google_services import Processor
from utils import Folders, EmailLabels


# Import these or define them if they are used globally in the module under test
# And then patch them directly where they are used.
# If these are defined in google_services.py, the patch will work.
# If they are from a separate 'constants' module, patch that module.
# Assuming they are directly in google_services.py for now, based on your previous logs.
# For the test, we'll patch the actual constant directly.


@pytest.fixture
def mock_creds():
    return Mock()
@pytest.fixture
def mock_folders():
    # Use the actual Folders class from your module, or define it fully here if it's simple
    # If Folders is an Enum or dataclass, import it. If it's just a dict, mock it as a dict.
    # Assuming utils.Folders is an Enum or has attributes like 'invoice' and 'unprocessed'
    mock_f = Mock()
    mock_f.invoice = 'invoice_folder_id'
    mock_f.unprocessed = 'unprocessed_folder_id'
    return mock_f # Return a mock object if it's typically accessed by attribute

@pytest.fixture
def mock_email_labels():
    # Similar to Folders, if it's a dict, use a dict. If it's an Enum/dataclass, import it.
    mock_e = Mock()
    mock_e.from_label = 'INBOX' # Use .from_label if that's what your code uses
    mock_e.to_label = 'Processed'    # Use .to if that's what your code uses
    return mock_e # Return a mock object if it's typically accessed by attribute

@pytest.fixture
def mock_animals_df():
    return pd.DataFrame({'ANIMALCODE': ['A1', 'A2'], 'OtherCol': [1, 2]})

# Add patches for the global constants as discussed in the previous answer
@patch('google_services.NON_INVOICE_REGEXES', new=r'ignore_this')
@patch('google_services.GMAIL_DATE', new='%Y/%m/%d')
@patch('google_services.GMAIL_DATE_ZONE', new='%Y/%m/%d %Z')
@patch('google_services.upload_dataframe_to_database', return_value=True)
@patch('google_services.match_animals')
@patch('google_services.get_parser') # This is the mock for the get_parser *function*
@patch('google_services.get_email_dates_sender', return_value=('sender@example.com', '2023-01-01'))
@patch('google_services.DriveService')
@patch('google_services.GmailService')
def test_processor_process_invoices_mocks(
    mock_gmail_service_class,
    mock_drive_service_class,
    mock_get_email_dates_sender,
    mock_get_parser, # This is the MagicMock replacing the actual get_parser function
    mock_match_animals,
    mock_upload_dataframe_to_database,
    mock_creds,
    mock_animals_df,
    mock_folders,
    mock_email_labels
):
    # --- Configure Mocks ---

    # Mock DriveService instance
    mock_drive_instance = mock_drive_service_class.return_value
    mock_drive_instance.get_or_create_folder.return_value = 'mock_drive_folder_id'
    mock_drive_instance.upload_file.return_value = 'mock_file_id'
    # Set up get_csv to return different IDs if needed, or adjust the update_csv_file assertions
    mock_drive_instance.get_csv.side_effect = [
        [{'id': 'mock_success_csv_id', 'name': 'successes.csv'}],
        [{'id': 'mock_failures_csv_id', 'name': 'failures.csv'}]
    ]
    mock_drive_instance.update_csv_file.return_value = 'mock_updated_csv_id'

    # Mock GmailService instance
    mock_gmail_instance = mock_gmail_service_class.return_value
    mock_gmail_instance.get_user_email.return_value = 'user@example.com'
    mock_gmail_instance.send_email_summary.return_value = True

    # Mock Gmail batch object and methods
    mock_batch_gmail = Mock()
    mock_gmail_instance.service.new_batch_http_request.return_value = mock_batch_gmail
    mock_batch_gmail.add.return_value = None
    mock_batch_gmail.execute.return_value = None

    # Mock get_message and its return value structure
    mock_message_payload = {
        "id": "mock_msg_id",
        "payload": {
            "headers": [
                {"name": "From", "value": "sender@example.com"},
                {"name": "Date", "value": "Tue, 01 Jan 2023 10:00:00 -0500"},
            ],
            "parts": [
                {
                    "filename": "test_invoice.pdf",
                    "body": {"attachmentId": "mock_att_id_1"},
                    "mimeType": "application/pdf"
                },
                {
                    "filename": "ignore_this.txt", # This should be skipped by regex
                    "body": {"attachmentId": "mock_att_id_2"},
                    "mimeType": "text/plain"
                },
                {
                    "filename": "invoice_non_match.pdf", # This should be processed
                    "body": {"attachmentId": "mock_att_id_3"},
                    "mimeType": "application/pdf"
                },
            ],
        }
    }
    mock_gmail_instance.get_message.return_value = mock_message_payload
    mock_gmail_instance.get_attachment.return_value = io.BytesIO(b"fake attachment data")

    mock_parser_instance_return = Mock()
    mock_parser_instance_return.parse_invoice.return_value = None
    mock_parser_instance_return.items = pd.DataFrame({
        'OriginalItem': ['Item A', 'Item B', 'Item C'],
        'Value': [10, 20, 30]
    })
    mock_parser_instance_return.drive_completed = 'CompletedFolder'
    mock_parser_instance_return.drive_incomplete = 'IncompleteFolder'

    mock_get_parser.return_value = mock_parser_instance_return


    mock_match_animals_df = pd.DataFrame({
        'OriginalItem': ['Item A', 'Item B', 'Item C'],
        'Value': [10, 20, 30],
        'ANIMALCODE': ['A1', 'ERROR_CODE', 'ERROR_CODE']
    })

    mock_match_animals.return_value = mock_match_animals_df
    mock_match_animals_success_df = pd.DataFrame({
        'OriginalItem': ['Item A_S', 'Item B_S'],
        'Value': [100, 200],
        'ANIMALCODE': ['A1_S', 'B2_S'] # No ERROR_CODE here
    })

    mock_match_animals_failure_df = pd.DataFrame({
        'OriginalItem': ['Item X_F', 'Item Y_F', 'Item Z_F'],
        'Value': [10, 20, 30],
        'ANIMALCODE': ['A1_F', 'ERROR_CODE', 'ERROR_CODE'] 
    })

    mock_match_animals.side_effect = [
        mock_match_animals_success_df, # First call for test_invoice.pdf
        mock_match_animals_failure_df  # Second call for invoice_non_match.pdf
        ]

    # --- Test Execution ---
    processor = Processor(mock_creds)
    test_messages = [{'id': 'mock_msg_id'}]

    # Call the method under test
    result = processor.process_invoices(
        messages=test_messages,
        folder_ids=mock_folders,
        email_labels=mock_email_labels,
        animals=mock_animals_df,
    )

    # --- Assertions (Examples) ---
    mock_drive_service_class.assert_called_once_with(mock_creds)
    mock_gmail_service_class.assert_called_once_with(mock_creds)
    mock_gmail_instance.get_message.assert_called_once_with("mock_msg_id")
    mock_gmail_instance.get_attachment.assert_any_call("mock_msg_id", "mock_att_id_1")
    mock_gmail_instance.get_attachment.assert_any_call("mock_msg_id", "mock_att_id_3")
    mock_get_email_dates_sender.assert_called_once()

    # Assert parsing and matching were attempted for invoice attachments (2 PDFs in payload)
    assert mock_get_parser.call_count == 2
    assert mock_match_animals.call_count == 2

    # Assert drive methods were called
    mock_drive_instance.get_or_create_folder.assert_any_call(name='CompletedFolder', parent_id='invoice_folder_id')
    mock_drive_instance.get_or_create_folder.assert_any_call(name='IncompleteFolder', parent_id='invoice_folder_id')
    
    # Check upload_file for successful and failed
    assert mock_drive_instance.upload_file.call_count == 2 # 1 for successful, 1 for failed
    mock_drive_instance.upload_file.assert_any_call(
        name='2023-01-01_sender@example.com_test_invoice.pdf', data=ANY, parents=['mock_drive_folder_id'], mime_type='application/pdf'
    )
    mock_drive_instance.upload_file.assert_any_call(
        name='2023-01-01_sender@example.com_invoice_non_match.pdf', data=ANY, parents=['mock_drive_folder_id'], mime_type='application/pdf'
    )


    # Assert Gmail batch and move message calls
    mock_gmail_instance.service.new_batch_http_request.assert_called_once()
    mock_gmail_instance.move_message.assert_called_once_with(msg_id="mock_msg_id", from_label='INBOX', to='Processed')
    mock_batch_gmail.add.assert_called_once_with(ANY)
    mock_batch_gmail.execute.assert_called_once()

    # Assert CSV report updates
    assert mock_drive_instance.get_csv.call_count == 2 # Once for successes, once for failures
    mock_drive_instance.get_csv.assert_any_call('invoice_folder_id', name_contains='successes')
    mock_drive_instance.get_csv.assert_any_call('invoice_folder_id', name_contains='failures')
    
    assert mock_drive_instance.update_csv_file.call_count == 2 # Once for successes, once for failures
    mock_drive_instance.update_csv_file.assert_any_call(file_id='mock_success_csv_id', new_data=ANY, new_name=ANY)
    mock_drive_instance.update_csv_file.assert_any_call(file_id='mock_failures_csv_id', new_data=ANY, new_name=ANY)


    mock_upload_dataframe_to_database.assert_called_once_with(ANY)
    mock_gmail_instance.send_email_summary.assert_called_once_with(ANY, 'user@example.com')
    assert result is True
