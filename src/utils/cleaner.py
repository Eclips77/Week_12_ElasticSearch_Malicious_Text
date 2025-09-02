class Preprocessor:
    """
    A utility class for preprocessing documents before indexing into Elasticsearch.
    """

    @staticmethod
    def normalize_antisemitic(docs: list[dict]) -> list[dict]:
        """
        Normalize the 'Antisemitic' field to boolean values.
        Converts 1/0 or "1"/"0" into True/False.
        Leaves True/False as is.
        """
        for doc in docs:
            if "Antisemitic" in doc:
                value = doc["Antisemitic"]
                if value in (1, "1"):
                    doc["Antisemitic"] = True
                elif value in (0, "0"):
                    doc["Antisemitic"] = False
        return docs
