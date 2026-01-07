
# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio

from google.cloud import storage
from google.cloud.storage._experimental.asyncio import AsyncMultiRangeDownloader


async def async_multirange_download(bucket_name, blob_name, destination_file_name):
    """Downloads a file from Google Cloud Storage using AsyncMultiRangeDownloader."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Define the ranges to download (e.g., first 1MB, second 1MB, etc.)
    ranges = "0-1048575,1048576-2097151"  # Example: Download the first 2MB in 1MB chunks

    downloader = AsyncMultiRangeDownloader(blob, ranges=ranges)

    try:
        async with open(destination_file_name, "wb") as file_obj:
            await downloader.download_to_file(file_obj)
        print(f"Downloaded {blob_name} from gs://{bucket_name} to {destination_file_name}")

    except Exception as e:
        print(f"Error downloading {blob_name}: {e}")


async def main():
    # Replace with your actual bucket name, blob name, and destination file name
    bucket_name = "your-bucket-name"
    blob_name = "your-blob-name"
    destination_file_name = "downloaded_file.txt"

    await async_multirange_download(bucket_name, blob_name, destination_file_name)


if __name__ == "__main__":
    asyncio.run(main())
