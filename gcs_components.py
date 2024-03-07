from xai_components.base import InArg, InCompArg, OutArg, Component, xai_component
from google.cloud import storage
from google.cloud.storage import Blob

@xai_component
class GCSUploadAndGetPublicUrl(Component):
    account_json_path: InCompArg[str]
    bucket_name: InArg[str]
    source_file_path: InArg[str]
    destination_blob_name: InArg[str]
    public_url: OutArg[str]

    def execute(self, ctx) -> None:
        storage_client = storage.Client.from_service_account_json(self.account_json_path.value)

        #storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name.value)
        blob = bucket.blob(self.destination_blob_name.value)

        blob.upload_from_filename(self.source_file_path.value)
        blob.make_public()

        self.public_url.value = blob.public_url

@xai_component
class GCSDownloadBlob(Component):
    bucket_name: InArg[str]
    source_blob_name: InArg[str]
    destination_file_path: InArg[str]

    def execute(self, ctx) -> None:
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name.value)
        blob = bucket.blob(self.source_blob_name.value)

        blob.download_to_filename(self.destination_file_path.value)

from datetime import datetime, timedelta

@xai_component
class GCSSignUrl(Component):
    bucket_name: InArg[str]
    blob_name: InArg[str]
    expiration_time: InArg[int]  # Expiration time in minutes
    signed_url: OutArg[str]

    def execute(self, ctx) -> None:
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name.value)
        blob = bucket.blob(self.blob_name.value)

        url = blob.generate_signed_url(
            expiration=datetime.now() + timedelta(minutes=self.expiration_time.value),
            version="v4"
        )

        self.signed_url.value = url