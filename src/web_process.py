from flask import Flask, request, redirect, url_for, session, render_template, jsonify
from animal_getter import get_probable_matches
import pandas as pd
import re


def add_invoices_col(fails: pd.DataFrame, pdfs: pd.DataFrame):
    cols = ['invoice', 'invoice_date']
    fails[cols] = fails['COSTDESCRIPTION'].str.extract(
        r" - (\d+) - (\d{4}-\d{2}-\d{2})")
    pdfs[cols] = pdfs['name'].str.extract(r"_(\d+)_(\d{4}-\d{2}-\d{2})")
    pdfs['cmp'] = pdfs['invoice'] + '_' + pdfs['invoice_date']
    fails['cmp'] = fails['invoice'] + '_' + fails['invoice_date']
    return fails, pdfs


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
