#!/bin/env python

import argparse
import base64
import binascii
import gzip
import json
from typing import Tuple, List, Union

import regex
from Crypto import Random
from Crypto.Cipher import AES
from Crypto.Util import Counter


def main():
    args = arguments()
    for batch_index in range(args.number_of_files):
        batch = batch_contents(args.topic, args.records_per_file, batch_index)
        body = gzip.compress(batch.encode())
        (iv, encrypted) = encrypt(body)
        encryption_metadata = encryption_metadata_contents(iv)
        with (open(output_filename(args.output_directory, args.topic, batch_index, "json.encryption.json"), "w")) as metadata:
            metadata.write(encryption_metadata)
        with (open(output_filename(args.output_directory, args.topic, batch_index, "json.gz.enc"), "wb")) as contents:
            contents.write(encrypted)


def encryption_metadata_contents(iv):
    return json.dumps({
        "initialisationVector": f"{iv}",
        "encryptedEncryptionKey": f"{encrypted_datakey()}",
        "keyEncryptionKeyId": "cloudhsm:262187,262209"
    }, indent=4)


def encrypt(payload: bytes) -> List[Union[str, bytes]]:
    initialisation_vector = Random.new().read(AES.block_size)
    iv_int = int(binascii.hexlify(initialisation_vector), 16)
    counter = Counter.new(AES.block_size * 8, initial_value=iv_int)
    aes = AES.new(base64.b64decode(plaintext_datakey()), AES.MODE_CTR, counter=counter)
    return [base64.b64encode(initialisation_vector).decode('ascii'), aes.encrypt(payload)]


def batch_contents(topic: str, number_of_records: int, batch_index: int) -> str:
    database, collection = database_and_collection(topic)
    return "\n".join(batch_list(batch_index, collection, database, number_of_records))


def batch_list(batch_index: int, collection: str, database: str, number_of_records: int) -> List[str]:
    return [record_contents(database, collection, batch_index, record_index) for record_index in
            range(number_of_records)]


def record_contents(database: str, collection: str, batch_index: int, record_index: int) -> str:
    dictionary = {
        "_id": {
            "id": f"{database}/{collection}/{batch_index}/{record_index}"
        },
        "type": "addressDeclaration",
        "contractId": "RANDOM_GUID",
        "addressNumber": {
            "type": "AddressLine",
        },
        "townCity": {
            "type": "AddressLine",
        },
        "postcode": "SM5 2LE",
        "processId": "RANDOM_GUID",
        "effectiveDate": {
            "type": "SPECIFIC_EFFECTIVE_DATE",
            "date": 20150320,
            "knownDate": 20150320
        },
        "paymentEffectiveDate": {
            "type": "SPECIFIC_EFFECTIVE_DATE",
            "date": 20150320,
            "knownDate": 20150320
        },
        "createdDateTime": {
            "$date": "2015-03-20T12:23:25.183Z"
        },
        "_version": 2,
        "_lastModifiedDateTime": {
            "$date": f'2018-12-01T15:01:02.{record_index:03d}Z'
        }
    }

    return json.dumps(dictionary)


def database_and_collection(topic: str) -> Tuple[str, str]:
    topic_regex = regex.compile(r"^(?:db\.)?((?:-|\w)+)\.((?:-|\w)+)")
    match = topic_regex.match(topic)
    if not match:
        raise ValueError(f"'{topic}' does not match '{topic_regex}'.")
    return match[1], match[2]


def plaintext_datakey():
    return "UBkbtizlrjYs5kZch3CwCg=="


def encrypted_datakey():
    return "kjiRV5fIgKMBI39KYCypACP4YCvk66LdOaMZ8P8jBNbu+i8I1Ji+nVlNR42TVjje5MISs5wX44n1vcE23YxuAuwme7uVJZ8rSKe0TIVDjuc/N8jZ/0eBsRZyndX8z7nHIOCuD2wkIndnIjIDJj4ve4AJpGu/CIufO+QnWiP+6/YyL9t2sCbdsyUQBL/4ub9NsuZMmmBJ4JAl6Fz/xpCXDB1fFaBKOB787YqeE5qbvwx+gnRgcXaRbvH7mxgnLhHEs9Ok/0tIiHoR91s7w5sDz5Neh4jvHsgkuO7EdSFS1l3LqW/2mjIIRsZDUjUauhidaSaM3MS8/xkI+wLAL0qGsA=="


def output_filename(output_directory: str, topic: str, batch: int, extension):
    return f"{output_directory}/{topic}.{batch:03d}.{extension}"


def arguments():
    parser = argparse.ArgumentParser(description="Create synthetic corporate data in the style persisted by k2hb.")
    parser.add_argument('-n', '--number-of-files', default=10, type=int)
    parser.add_argument('-r', '--records-per-file', default=100, type=int)
    parser.add_argument('-o', '--output-directory', default="ephemera", type=str)
    parser.add_argument('topic')
    return parser.parse_args()


if __name__ == '__main__':
    main()
