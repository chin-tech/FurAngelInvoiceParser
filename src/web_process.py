import re
import pandas as pd
from datetime import datetime as dt

from flask import Request, Response, render_template

from animal_db_handler import get_probable_matches, upload_dataframe_to_database
from google_services import DriveService
from utils import error_logger


def show_failed_invoices(
    bad_invoice: pd.DataFrame, pdfs: pd.DataFrame, animals: pd.DataFrame,
) -> Response:
    # bad_invoice, pdfs = add_invoices_col(bad_invoice, pdfs)
    bad_invoice["date"] = pd.to_datetime(bad_invoice["COSTDATE"])
    bad_invoice["name"] = bad_invoice["ANIMALNAME"]
    bad_invoice = bad_invoice.sort_values(by="name")
    link_map = pdfs.set_index("cmp")["webViewLink"]
    bad_invoice["link"] = bad_invoice["cmp"].map(link_map)
    fails = bad_invoice[
        ["name", "invoice", "link", "date", "COSTDATE"]
    ].drop_duplicates(["name", "invoice"])

    data = []
    for row in fails.itertuples():
        likely_animals = get_probable_matches(row.name, animals, row.date)
        data.append((row, likely_animals.to_dict(orient="records")))
    return Response(
        render_template(
            "get.html", data_to_show=data, animal_df=animals.to_json(orient="records"),
        ),
        200,
    )


def get_post_data(req, animals: pd.DataFrame) -> pd.DataFrame:
    data = []
    for key, value in req.form.items():
        if key.startswith("new_animal_"):
            idx = "_".join(re.findall(r"(\d+)", key))
            name = value
            code = animals[animals["ANIMALNAME"] == name]["SHELTERCODE"].values[0]
            assert code is not None
            data.append(
                {
                    "index": idx,
                    "name": name,
                    "sheltercode": code,
                },
            )
        elif re.search(r"^\b\d+\b", key):
            idx = key
            code = value
            name = animals[animals["SHELTERCODE"] == code]["ANIMALNAME"].values[0]
            assert name is not None
            data.append(
                {
                    "index": idx,
                    "name": name,
                    "sheltercode": code,
                },
            )
    df = pd.DataFrame(data)
    df["index"] = df["index"].astype("str")
    df["indices"] = df["index"].str.extract(r"(\d+)_?")
    df["indices"] = df["indices"].astype("int")
    return df


def update_invoice_data(in_data: pd.DataFrame, corrected: pd.DataFrame) -> pd.DataFrame:
    invoice = in_data.copy()
    if corrected.empty:
        return invoice
    cgroups = corrected.groupby("indices")

    for invoice_idx in invoice.index:
        if invoice_idx in cgroups.groups:
            correct_group = cgroups.get_group(invoice_idx)
            matched = invoice.iloc[invoice_idx]
            # print(matched)
            indices = invoice[
                (invoice["ANIMALNAME"] == matched["ANIMALNAME"])
                & (invoice["invoice"] == matched["invoice"])
            ].index

            if indices.empty:
                continue

            if len(correct_group) == 1:
                name, code = correct_group.iloc[0][["name", "sheltercode"]]

                invoice.loc[indices, ["ANIMALNAME", "ANIMALCODE"]] = name, code
            else:
                row_amount = len(correct_group)
                for _, row in correct_group.iterrows():
                    nrow = invoice.iloc[indices].copy()
                    new_index = pd.RangeIndex(
                        invoice.shape[0], invoice.shape[0] + nrow.shape[0],
                    )
                    nrow[["ANIMALNAME", "ANIMALCODE"]] = row[["name", "sheltercode"]]
                    nrow["COSTAMOUNT"] /= row_amount
                    if nrow.shape[0] == 1:
                        nrow.name = invoice.shape[0]
                        invoice = pd.concat([invoice, nrow.to_frame().T])
                        continue
                    nrow.index = new_index
                    invoice = pd.concat([invoice, nrow])
                invoice = invoice.drop(indices)
    return invoice


#! TODO: Have to update google drive function
@error_logger()
def upload_corrected_files(
    drive: DriveService, parent_folder: str, fails, goods,
) -> tuple[str, str]:
    timestamp = dt.now().strftime("%Y-%m-%d-%H:%M:%S")
    drop_cols = ["invoice", "invoice_date", "cmp"]

    failures_name = f"{timestamp}_failures.csv"
    success_name = f"{timestamp}_corrections.csv"
    corrections_folder = drive.get_or_create_folder(
        name="corrections",
        parent_id=parent_folder
    )

    old_failure_csv_list = drive.get_csv(
        parent_id=parent_folder,
        name_contains='_failures'
    )
    if len(old_failure_csv_list) != 1:
        raise Exception("Too many failure CSVs in Invoice folder!")
    old_csv = old_failure_csv_list[0]
    print(
        f"{old_csv.get('id')} | {old_csv.get('name')}"
    )
    print(f"Attempting to move csv FROM: {parent_folder} -> {corrections_folder}")
    new_name = f"{old_csv.get('name')}.bak"

    ## Create a backup of failures csv ##
    movement_id = drive.move_file(
        id=old_csv.get('id'),
        old_parents=[parent_folder],
        new_parents=[corrections_folder],
        new_name=new_name,
        execute=True
    )
    if movement_id:
        print(f"Successfully moved: {old_csv.get('name')}: {movement_id}")

    failure_id = drive.upload_file(
        name=failures_name,
        data=fails.drop(drop_cols, axis=1),
        mime_type='csv',
        parents=[parent_folder]
    )
    if failure_id:
        pass

    good_df = goods.drop(drop_cols, axis=1)
    success_id = drive.upload_file(
        name=success_name,
        data=good_df,
        mime_type='csv',
        parents=[corrections_folder]
    )

    if success_id:
        upload_dataframe_to_database(good_df)

    return failure_id, success_id


def cleanup_old_failed_invoice(
    drive: DriveService, parent_folder: str, pdfs, goods, fails,
) -> None:
    """Cleans up old failed invoice file and moves completed invoices into appropriate folders."""
    batch = drive.service.new_batch_http_request()
    folders = pd.DataFrame(drive.get_folders(parent_folder))

    completed_pdfs = goods["cmp"].unique()
    incomplete_pdfs = fails["cmp"].unique()

    updated_invoices = pdfs[
        (pdfs["cmp"].isin(completed_pdfs)) & (~pdfs["cmp"].isin(incomplete_pdfs))
    ]
    for pdf in updated_invoices.itertuples():
        invoice_type = pdf.name.split("_")[0]
        try:
            incomplete_folder = folders[
                folders["name"] == f"{invoice_type}_incomplete"
            ]["id"].values[0]
            complete_folder = folders[folders["name"] == f"{invoice_type}_completed"][
                "id"
            ].values[0]
            batch.add(
                drive.move_file(
                    id=pdf.id,
                    old_parents=[incomplete_folder],
                    new_parents=[complete_folder],
                )
            )
        except Exception:
            continue
    batch.execute()


def process_invoice_corrections(
    drive: DriveService, req: Request, parent_folder: str, failed, pdfs, animals,
) -> Response:
    post_df = get_post_data(req, animals)
    updated = update_invoice_data(failed, post_df)

    good_condition = updated["ANIMALCODE"] != "ERROR_CODE"
    to_upload = updated[good_condition]
    to_fails = updated[~good_condition]

    error, success = upload_corrected_files(drive, parent_folder, to_fails, to_upload)
    if success:
        cleanup_old_failed_invoice(drive, parent_folder, pdfs, to_upload, to_fails)

    return Response(render_template(
        "post.html", invoices=post_df.shape[0], rows=to_upload.shape[0],
    ))
