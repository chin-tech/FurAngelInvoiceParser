import re
from fuzzywuzzy import fuzz, process


def find_best_match(text: str, choices: list[str], scorer=None, threshold=51) -> str:
    if not scorer:
        scorer = fuzz.WRatio
    try:
        match, score = process.extractOne(
            text, choices, score_cutoff=threshold)
        return match if score >= 50 else ""
    except Exception:
        return ""


class Vaccine:
    DHLPP = "DHLPP"
    DHPP = "DHPP"
    BORDETELLA = "Bordetella"
    LEPTOSPIROSIS = "Leptospirosis"
    PARAINFLUENZA = "Parainfluzena"
    options = [
        "DHLPP",
        "DHPP",
        "Bordetella",
        "Leptospirosis",
        "Parainfluenza",
        "Leptospira",
        "Giardia",
        "Torigen",
    ]

    def parse(self, txt: str) -> str:
        txt = txt.lower()
        txt = re.sub(
            r"(vaccine|vaccination|litter|1st|2nd|3rd|booster|adult|puppy|no lepto)", "", txt).strip()
        if re.search(r"dhpp|da2pp|da2p-pv", txt):
            return "DHPP"
        if re.search(r"kennel cough", txt):
            return "Bordetella"
        return find_best_match(txt, self.options)


# Cost Types #
class Cost:
    EXAMINATION = "Examination"
    EMERGENCY = "Emergency Room"
    SURGERY = "Surgery"
    MEDICATION = "Medication"
    FOOD = "Food"
    TEST = "Medical Test"
    VACCINATION = "Vaccination"
    SPAY_NEUTER = "Spay/Neuter"
    SUPPLIES = "Supplies"
    GROOMING = "Grooming"
    MICROCHIP = "Microchip"
    BANDAGE = "Bandages"
    EUTHANASIA = "Euthanasia"
    OTHER = "Other"


class Test:
    HEARTWORM = "Heartworm"
    BLOODWORK = "Bloodwork"
    FECAL = "Fecal"
    SCRAPE = "Skin Scrape"
    CYTOLOGY = "Cytology"
    options = [
        "Biopsy",
        "Bloodwork",
        "Cytology",
        "Opthamalogy",
        "Fecal",
        "Fungal",
        "Glucose",
        "Heartworm",
        "Lactate",
        "Lyme",
        "Parvo",
        "Radiology",
        "Skin Scrape",
        "Ultrasound",
        "Wood's Light",
        "Urine",
        "Tonometry",
        "Echocardiogram",
    ]

    def parse(self, txt: str) -> str | None:
        if "biopsy" in txt:
            return "Biopsy"
        if re.search(r"cbc|cpl|idx", txt):
            return "Bloodwork"
        if "hw" in txt:
            return "Heartworm"
        if re.search(r"tear|eye|opth", txt):
            return "Opthamalogy"
        if "ua" in txt:
            return "Urine"
        if re.search("gi|gastro", txt):
            return "Fecal"
        if re.search("tick|lyme", txt):
            return "Lyme"
        if re.search(r"x-?ray", txt):
            return "Radiology"
        return find_best_match(txt, self.options)


class Medication:
    APOQUEL = "Apoquel"
    AMOXICILLIN = "Amoxicillin"
    SIMPARICA = "Simparica"
    CYTOPOINT = "Cytopoint"
    CEFPODERM = "Cefpoderm"
    CLAVACILLIN = "Clavacillin"
    CERENIA = "Cerenia"
    POLYFLEX = "Polyflex"
    BRAVECTO = "Bravecto"
    CLAVOMAX = "Clavamax"
    NEXGARD = "Nexgard"
    INTERCEPTOR = "Interceptor Plus"
    HEARTGARD = "Heartgard"
    REVOLUTION = "Revolution"
    BRAVECTO = "Bravecto"
    PANACUR = "Panacur"
    SENTINEL = "Sentinel"
    RIMADYL = "Rimadyl"
    METRONIDAZOLE = "Metronidazole"
    TRAZODONE = "Trazodone"
    TRESADERM = "Tresaderm"
    GABAPENTIN = "Gabapentin"
    GALLIPRANT = "Galliprant"
    DOXYCYCLINE = "Doxycycline"
    SEVOFLURANE = "Sevoflurane"
    SIMPLICIEF = "Simplicef"
    KETOCONAZOLE = "Ketoconazole"

    options = [
        "Adequan",
        "Amikacin",
        "Aminocaproic",
        "Amoxicillin",
        "Amoxiclay",
        "Ampicillin",
        "Apoquel",
        "Bedinvetmab",
        "Bravecto",
        "Bravecto",
        "Bupivacaine",
        "Buprenorphine",
        "Capromorelin",
        "Capstar",
        "Carprofen",
        "Cefazolin",
        "Cefovecin",
        "Cefpoderm",
        "Cerenia",
        "Cevofecin",
        "Clavacillin",
        "Clavamax",
        "Clindamycin",
        "Codeine",
        "Cyclosporine",
        "Cytopoint",
        "Denamarin",
        "Dexamethasone",
        "Dextrose",
        "Dipenhydramine",
        "Dorzolamide",
        "Doxycycline",
        "Enrofloxacin",
        "Famotidine",
        "Fetanyl",
        "Furosemide",
        "Gabapentin",
        "Galliprant",
        "Gentamicin",
        "Heartgard",
        "Hydromorphone",
        "Insulin",
        "Interceptor Plus",
        "Ketoconazole",
        "Latantoprost",
        "Levothyroxine",
        "Librela",
        "Marbofloxacin",
        "Meclizine",
        "Meloxicam",
        "Methadone",
        "Metronidazole",
        "Miconazole",
        "Mometamax",
        "Moxidectin",
        "Neopolybacitracin",
        "Nexgard",
        "Optixcare",
        "Panacur",
        "Pantoprazole",
        "Pimobendan",
        "Polyflex",
        "Ponazuril",
        "Prazpyrfeb",
        "Prednisone",
        "Pyrantel",
        "Revolution",
        "Rimadyl",
        "Sentinel",
        "Sevoflurane",
        "Simparica",
        "Simplicef",
        "Sucralfate",
        "Sulfadimethoxine",
        "Tacrolimus",
        "Tobramycin",
        "Trazodone",
        "Tresaderm",
        "Triamcinolone",
        "Ursodiol",
        "Electrolytes"
        "Vitamin",
    ]

    def parse(self, txt: str) -> str:
        if re.search(r"kcl", txt):
            return "Electrolytes"
        if re.search(r"vit k1|vitamin", txt):
            return "Vitamin"
        return find_best_match(txt, self.options)
