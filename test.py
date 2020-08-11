from otri.downloader.tradier import TradierRealtime
from otri.database.postgresql_adapter import PostgreSQLAdapter
from otri.importer.default_importer import DefaultImporter
from otri.utils import config
import multiprocessing
import time

if __name__ == "__main__":
    # Test code here. Revert before committing.
    downloader = TradierRealtime("uX6GAw8IGMtlcZfV7k3S2eLsGaGx")

    # Setup database connection
    db_adapter = PostgreSQLAdapter(
        host=config.get_value("postgresql_host"),
        port=config.get_value("postgresql_port", "5432"),
        user=config.get_value("postgresql_username", "postgres"),
        password=config.get_value("postgresql_password"),
        database=config.get_value("postgresql_database", "postgres")
    )

    importer = DefaultImporter(db_adapter)
    downloader.start(["AAPL", "GOOG", "ENI/MI"], importer=importer, period=1)
