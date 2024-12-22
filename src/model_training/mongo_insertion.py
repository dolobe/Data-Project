import pymongo
from pymongo.errors import DuplicateKeyError

class MongoDBHandler:
    def __init__(self, uri, db_name, collection_name):
        """
        Initialize MongoDBHandler to manage interactions with MongoDB.

        Parameters:
        - uri (str): MongoDB connection URI.
        - db_name (str): Name of the database.
        - collection_name (str): Name of the collection.
        """
        self.client = pymongo.MongoClient(uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def insert_one(self, data):
        """
        Insert a single document into the MongoDB collection.

        Parameters:
        - data (dict): The document to insert.
        """
        try:
            result = self.collection.insert_one(data)
            print(f"Document inserted with ID: {result.inserted_id}")
        except DuplicateKeyError:
            print("Duplicate document found. Skipping insertion.")
        except Exception as e:
            print(f"Error inserting document: {e}")

    def insert_many(self, data_list):
        """
        Insert multiple documents into the MongoDB collection.

        Parameters:
        - data_list (list): List of documents to insert.
        """
        try:
            result = self.collection.insert_many(data_list, ordered=False)
            print(f"{len(result.inserted_ids)} documents inserted successfully.")
        except DuplicateKeyError:
            print("Duplicate documents detected. Skipping duplicate insertions.")
        except Exception as e:
            print(f"Error inserting documents: {e}")

    def find(self, query=None, projection=None):
        """
        Retrieve documents from the MongoDB collection.

        Parameters:
        - query (dict): MongoDB query to filter results (default: {}).
        - projection (dict): Fields to include or exclude (default: None).

        Returns:
        - list: List of documents matching the query.
        """
        if query is None:
            query = {}
        try:
            documents = list(self.collection.find(query, projection))
            print(f"{len(documents)} documents retrieved.")
            return documents
        except Exception as e:
            print(f"Error retrieving documents: {e}")
            return []

    def delete(self, query):
        """
        Delete documents from the MongoDB collection.

        Parameters:
        - query (dict): MongoDB query to filter documents for deletion.
        """
        try:
            result = self.collection.delete_many(query)
            print(f"{result.deleted_count} documents deleted.")
        except Exception as e:
            print(f"Error deleting documents: {e}")

    def update(self, query, update_fields):
        """
        Update documents in the MongoDB collection.

        Parameters:
        - query (dict): MongoDB query to filter documents to update.
        - update_fields (dict): Fields to update in the matching documents.
        """
        try:
            result = self.collection.update_many(query, {"$set": update_fields})
            print(f"{result.modified_count} documents updated.")
        except Exception as e:
            print(f"Error updating documents: {e}")

    def count_documents(self, query=None):
        """
        Count the number of documents matching a query.

        Parameters:
        - query (dict): MongoDB query to filter results (default: {}).

        Returns:
        - int: Number of matching documents.
        """
        if query is None:
            query = {}
        try:
            count = self.collection.count_documents(query)
            print(f"{count} documents match the query.")
            return count
        except Exception as e:
            print(f"Error counting documents: {e}")
            return 0
