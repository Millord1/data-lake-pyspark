from src.clean.run_clean import clean_velib
from src.ingestion.velib_histo import VelibHistoryIngestor
from src.ingestion.velib_ingestion import VelibDataIngestor


def run_ingestion():
    ingestor = VelibDataIngestor()
    ingestor.run_pipeline()

    hist_ingestor = VelibHistoryIngestor()
    hist_ingestor.run()

    clean_velib()
