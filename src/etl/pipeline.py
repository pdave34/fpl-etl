from etl.extractors import FPL
from etl.loaders import OracleDBLoader


class Pipeline:
    def __init__(self) -> None:
        self.extractor = FPL()
        self.loader = OracleDBLoader()

    def rebuild(self, table_prefix: str = "FPL") -> None:
        for k, df in self.extractor.generate():
            data = df.to_arrow()
            table_name = f"{table_prefix}_{k.upper()}"
            self.loader.rebuild_table(table_name=table_name, data=data)

    def reload(self, table_prefix: str = "FPL") -> None:
        for k, df in self.extractor.generate():
            data = df.to_arrow()
            table_name = f"{table_prefix}_{k.upper()}"
            self.loader.reload_table(table_name=table_name, data=data)
