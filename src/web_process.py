import re
import pandas as pd
from flask import render_template, Response, Request
from datetime import datetime as dt
from gfuncs import Google
from animal_getter import get_probable_matches
from animal_getter import upload_dataframe_to_database


def show_failed_invoices(bad_invoice: pd.DataFrame, pdfs: pd.DataFrame, animals: pd.DataFrame):
    # bad_invoice, pdfs = add_invoices_col(bad_invoice, pdfs)
    bad_invoice['date'] = pd.to_datetime(bad_invoice['COSTDATE'])
    bad_invoice['name'] = bad_invoice['ANIMALNAME']
    bad_invoice.sort_values(by='name', inplace=True)
    link_map = pdfs.set_index('cmp')['webViewLink']
    bad_invoice['link'] = bad_invoice['cmp'].map(link_map)
    fails = bad_invoice[['name', 'invoice', 'link', 'date', 'COSTDATE']
                        ].drop_duplicates(['name', 'invoice'])

    data = []
    for row in fails.itertuples():
        likely_animals = get_probable_matches(row.name, animals, row.date)
        data.append((row, likely_animals.to_dict(orient='records')))
    return render_template('get.html', data_to_show=data, animal_df=animals.to_json(orient='records'))


def get_post_data(req, animals: pd.DataFrame) -> pd.DataFrame:
    data = []
    for key, value in req.form.items():
        if key.startswith('new_animal_'):
            idx = '_'.join(re.findall(r"(\d+)", key))
            name = value
            code = animals[animals['ANIMALNAME']
                           == name]['SHELTERCODE'].values[0]
            assert code != None
            data.append({
                'index': idx,
                'name': name,
                'sheltercode': code,
            })
        elif re.search(r"^\b\d+\b", key):
            idx = key
            code = value
            name = animals[animals['SHELTERCODE']
                           == code]['ANIMALNAME'].values[0]
            assert name != None
            data.append({
                'index': idx,
                'name': name,
                'sheltercode': code,
            })
    df = pd.DataFrame(data)
    df['index'] = df['index'].astype('str')
    df['indices'] = df['index'].str.extract(r"(\d+)_?")
    df['indices'] = df['indices'].astype('int')
    return df


def update_invoice_data(in_data: pd.DataFrame, corrected: pd.DataFrame) -> pd.DataFrame:
    invoice = in_data.copy()
    if corrected.empty:
        return invoice
    cgroups = corrected.groupby('indices')

    for invoice_idx in invoice.index:
        if invoice_idx in cgroups.groups:
            correct_group = cgroups.get_group(invoice_idx)
            matched = invoice.iloc[invoice_idx]
            # print(matched)
            indices = invoice[
                (invoice['ANIMALNAME'] == matched['ANIMALNAME']) &
                (invoice['invoice'] == matched['invoice'])
            ].index

            if indices.empty:
                continue

            if len(correct_group) == 1:
                name, code = correct_group.iloc[0][['name', 'sheltercode']]

                invoice.loc[indices, ['ANIMALNAME', 'ANIMALCODE']
                            ] = name, code
            else:
                row_amount = len(correct_group)
                for _, row in correct_group.iterrows():
                    nrow = invoice.iloc[indices].copy()
                    new_index = pd.RangeIndex(
                        invoice.shape[0], invoice.shape[0] + nrow.shape[0])
                    nrow[['ANIMALNAME', 'ANIMALCODE']
                         ] = row[['name', 'sheltercode']]
                    nrow['COSTAMOUNT'] /= row_amount
                    if nrow.shape[0] == 1:
                        nrow.name = invoice.shape[0]
                        invoice = pd.concat([invoice, nrow.to_frame().T])
                        continue
                    nrow.index = new_index
                    invoice = pd.concat([invoice, nrow])
                invoice.drop(indices, inplace=True)
    return invoice


def upload_corrected_files(google: Google, parent_folder: str, fails, goods) -> (str, str):
    timestamp = dt.now().strftime("%Y-%m-%d-%H:%M:%S")
    drop_cols = ['invoice', 'invoice_date', 'cmp']

    error_name = f'{timestamp}_failures.csv'
    success_name = f'{timestamp}_corrections.csv'
    corrections_folder = google.get_drive_folder('corrections', parent_folder)

    old_invoices = google.get_failures_csv(parent_folder)
    # Remove old_failures into corrections folder
    for invoice in old_invoices:
        google.drive.files().update(
            fileId=invoice.get('id'),
            body={'name': f"{invoice.get('name')}.bak"},
            removeParents=parent_folder,
            addParents=corrections_folder,
        ).execute()
        print(f"Moved: {invoice.get('name')} -> 'corrections' folder")

    error_id = google.upload_drive(
        fails.drop(drop_cols, axis=1),
        error_name, [parent_folder], 'text/csv'
    )
    if error_id:
        print("Successfully Uploaded New Failures CSV")

    success_id = google.upload_drive(
        goods.drop(drop_cols, axis=1),
        success_name, [corrections_folder], 'text/csv'
    )

    if success_id:
        uploaded = upload_dataframe_to_database(goods.drop(drop_cols, axis=1))
        print(f"ASM data Uploaded: {uploaded}")

    return error_id, success_id


def cleanup_old_failed_invoice(google: Google, parent_folder: str, pdfs, goods, fails):
    """ Cleans up old failed invoice file and moves completed invoices into appropriate folders"""

    batch = google.drive.new_batch_http_request()
    folders = pd.DataFrame(google.get_invoice_folders(parent_folder))

    completed_pdfs = goods['cmp'].unique()
    incomplete_pdfs = fails['cmp'].unique()

    updated_invoices = pdfs[
        (pdfs['cmp'].isin(completed_pdfs)) &
        (~pdfs['cmp'].isin(incomplete_pdfs))
    ]
    for pdf in updated_invoices.itertuples():
        invoice_type = pdf.name.split('_')[0]
        incomplete_folder = folders[folders['name'] == f"{
            invoice_type}_incomplete"]['id'].values[0]
        complete_folder = folders[folders['name'] == f"{
            invoice_type}_completed"]['id'].values[0]
        batch.add(google.drive.files().update(
            fileId=pdf.id,
            addParents=[complete_folder],
            removeParents=[incomplete_folder],
        ))
    batch.execute()
    global GLOBAL_CREDS
    GLOBAL_CREDS = None


def process_invoice_corrections(google: Google, req: Request, parent_folder: str, failed, pdfs, animals) -> Response:
    post_df = get_post_data(req, animals)
    updated = update_invoice_data(failed, post_df)

    good_condition = updated['ANIMALCODE'] != 'ERROR_CODE'
    to_upload = updated[good_condition]
    to_fails = updated[~good_condition]

    error, success = upload_corrected_files(
        google, parent_folder, to_fails, to_upload)
    if success:
        cleanup_old_failed_invoice(
            google, parent_folder, pdfs, to_upload, to_fails)

    return render_template('post.html', invoices=post_df.shape[0], rows=to_upload.shape[0])
