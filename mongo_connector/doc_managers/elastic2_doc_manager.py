# Copyright 2016 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Elasticsearch implementation of the DocManager interface.

Receives documents from an OplogThread and takes the appropriate actions on
Elasticsearch.
"""
import base64
import logging
import warnings

from threading import Timer,Lock

import bson.json_util

from copy import deepcopy

from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch.helpers import bulk, scan, streaming_bulk

from mongo_connector import errors
from mongo_connector.compat import u
from mongo_connector.constants import (DEFAULT_COMMIT_INTERVAL,
                                       DEFAULT_MAX_BULK)
from mongo_connector.util import exception_wrapper, retry_until_ok
from mongo_connector.doc_managers.doc_manager_base import DocManagerBase
from mongo_connector.doc_managers.formatters import DefaultDocumentFormatter

wrap_exceptions = exception_wrapper({
    es_exceptions.ConnectionError: errors.ConnectionFailed,
    es_exceptions.TransportError: errors.OperationFailed,
    es_exceptions.NotFoundError: errors.OperationFailed,
    es_exceptions.RequestError: errors.OperationFailed})

LOG = logging.getLogger(__name__)
Formatter = DefaultDocumentFormatter()


class DocManager(DocManagerBase):
    """Elasticsearch implementation of the DocManager interface.

    Receives documents from an OplogThread and takes the appropriate actions on
    Elasticsearch.
    """

    def __init__(self, url, auto_commit_interval=DEFAULT_COMMIT_INTERVAL,
                 unique_key='_id', chunk_size=DEFAULT_MAX_BULK,
                 meta_index_name="mongodb_meta", meta_type="mongodb_meta",
                 attachment_field="content", **kwargs):
        self.elastic = Elasticsearch(
            hosts=[url], **kwargs.get('clientOptions', {}))
        
        self.BulkBuffer = BulkBuffer(self)
        self.lock = Lock()
        
        self.auto_commit_interval = auto_commit_interval
        self.meta_index_name = meta_index_name
        self.meta_type = meta_type
        self.unique_key = unique_key
        self.chunk_size = chunk_size
        if self.auto_commit_interval not in [None, 0]:
            self.run_auto_commit()

        self.has_attachment_mapping = False
        self.attachment_field = attachment_field

    def _index_and_mapping(self, namespace):
        """Helper method for getting the index and type from a namespace."""
        index, doc_type = namespace.split('.', 1)
        return index.lower(), doc_type

    def stop(self):
        """Stop the auto-commit thread."""
        self.auto_commit_interval = None

    def apply_update(self, doc, update_spec):
        if "$set" not in update_spec and "$unset" not in update_spec:
            # Don't try to add ns and _ts fields back in from doc
            return update_spec
        return super(DocManager, self).apply_update(doc, update_spec)

    @wrap_exceptions
    def handle_command(self, doc, namespace, timestamp):
        db = namespace.split('.', 1)[0]
        if doc.get('dropDatabase'):
            dbs = self.command_helper.map_db(db)
            for _db in dbs:
                self.elastic.indices.delete(index=_db.lower())

        if doc.get('renameCollection'):
            raise errors.OperationFailed(
                "elastic_doc_manager does not support renaming a mapping.")

        if doc.get('create'):
            db, coll = self.command_helper.map_collection(db, doc['create'])
            if db and coll:
                self.elastic.indices.put_mapping(
                    index=db.lower(), doc_type=coll,
                    body={
                        "_source": {"enabled": True}
                    })

        if doc.get('drop'):
            db, coll = self.command_helper.map_collection(db, doc['drop'])
            if db and coll:
                # This will delete the items in coll, but not get rid of the
                # mapping.
                warnings.warn("Deleting all documents of type %s on index %s."
                              "The mapping definition will persist and must be"
                              "removed manually." % (coll, db))
                responses = streaming_bulk(
                    self.elastic,
                    (dict(result, _op_type='delete') for result in scan(
                        self.elastic, index=db.lower(), doc_type=coll)))
                for ok, resp in responses:
                    if not ok:
                        LOG.error(
                            "Error occurred while deleting ElasticSearch docum"
                            "ent during handling of 'drop' command: %r" % resp)

    @wrap_exceptions
    def update(self, document_id, update_spec, namespace, timestamp):
        """Apply updates given in update_spec to the document whose id
        matches that of doc.
        """

        index, doc_type = self._index_and_mapping(namespace)
        document = self.BulkBuffer.get_from_sources(index,doc_type,u(document_id))
        if document:
            updated = self.apply_update(document, update_spec)
            # _id is immutable in MongoDB, so won't have changed in update
            updated['_id'] = document_id
            self.upsert(updated, namespace, timestamp)
        else:
            updated = {"_id": document_id}
            self.upsert(updated, namespace, timestamp, update_spec)
        # upsert() strips metadata, so only _id + fields in _source still here
        return updated
    
    @wrap_exceptions
    def upsert(self, doc, namespace, timestamp, update_spec=None):
        """Insert a document into Elasticsearch."""
        index, doc_type = self._index_and_mapping(namespace)
        # No need to duplicate '_id' in source document
        doc_id = u(doc.pop("_id"))
        metadata = {
            'ns': namespace,
            '_ts': timestamp
        }
        
        # Index the source document, using lowercase namespace as index name.
        action = {
            '_op_type': 'index',
            '_index': index,
            '_type': doc_type,
            '_id': doc_id,
            '_source': Formatter.format_document(doc)
        }
        # Index document metadata with original namespace (mixed upper/lower).
        meta_action = {
            '_op_type': 'index',
            '_index': self.meta_index_name,
            '_type': self.meta_type,
            '_id': doc_id,
            '_source': bson.json_util.dumps(metadata)
        }
        
        self.index(action,meta_action,doc,update_spec)
        
        # Leave _id, since it's part of the original document
        doc['_id'] = doc_id

    @wrap_exceptions
    def bulk_upsert(self, docs, namespace, timestamp):
        """Insert multiple documents into Elasticsearch."""
        def docs_to_upsert():
            doc = None
            for doc in docs:
                # Remove metadata and redundant _id
                index, doc_type = self._index_and_mapping(namespace)
                doc_id = u(doc.pop("_id"))
                document_action = {
                    '_index': index,
                    '_type': doc_type,
                    '_id': doc_id,
                    '_source': Formatter.format_document(doc)
                }
                document_meta = {
                    '_index': self.meta_index_name,
                    '_type': self.meta_type,
                    '_id': doc_id,
                    '_source': {
                        'ns': namespace,
                        '_ts': timestamp
                    }
                }
                yield document_action
                yield document_meta
            if doc is None:
                raise errors.EmptyDocsError(
                    "Cannot upsert an empty sequence of "
                    "documents into Elastic Search")
        try:
            kw = {}
            if self.chunk_size > 0:
                kw['chunk_size'] = self.chunk_size

            responses = streaming_bulk(client=self.elastic,
                                       actions=docs_to_upsert(),
                                       **kw)

            for ok, resp in responses:
                if not ok:
                    LOG.error(
                        "Could not bulk-upsert document "
                        "into ElasticSearch: %r" % resp)
            
            if self.auto_commit_interval == 0:
                self.commit()
            
        except errors.EmptyDocsError:
            # This can happen when mongo-connector starts up, there is no
            # config file, but nothing to dump
            pass

    @wrap_exceptions
    def insert_file(self, f, namespace, timestamp):
        doc = f.get_metadata()
        doc_id = str(doc.pop('_id'))
        index, doc_type = self._index_and_mapping(namespace)

        # make sure that elasticsearch treats it like a file
        if not self.has_attachment_mapping:
            body = {
                "properties": {
                    self.attachment_field: {"type": "attachment"}
                }
            }
            self.elastic.indices.put_mapping(index=index,
                                             doc_type=doc_type,
                                             body=body)
            self.has_attachment_mapping = True

        metadata = {
            'ns': namespace,
            '_ts': timestamp,
        }

        doc = Formatter.format_document(doc)
        doc[self.attachment_field] = base64.b64encode(f.read()).decode()

        self.elastic.index(index=index, doc_type=doc_type,
                           body=doc, id=doc_id,
                           refresh=(self.auto_commit_interval == 0))
        self.elastic.index(index=self.meta_index_name, doc_type=self.meta_type,
                           body=bson.json_util.dumps(metadata), id=doc_id,
                           refresh=(self.auto_commit_interval == 0))

    @wrap_exceptions
    def remove(self, document_id, namespace, timestamp):
        """Remove a document from Elasticsearch."""
        index, doc_type = self._index_and_mapping(namespace)

        action = {
            '_op_type': 'delete',
            '_index': index,
            '_type': doc_type,
            '_id': u(document_id)
        }

        meta_action = {
            '_op_type': 'delete',
            '_index': self.meta_index_name,
            '_type': self.meta_type,
            '_id': u(document_id)
        }

        self.index(action, meta_action)

    @wrap_exceptions
    def _stream_search(self, *args, **kwargs):
        """Helper method for iterating over ES search results."""
        for hit in scan(self.elastic, query=kwargs.pop('body', None),
                        scroll='10m', **kwargs):
            hit['_source']['_id'] = hit['_id']
            yield hit['_source']

    def search(self, start_ts, end_ts):
        """Query Elasticsearch for documents in a time range.

        This method is used to find documents that may be in conflict during
        a rollback event in MongoDB.
        """
        return self._stream_search(
            index=self.meta_index_name,
            body={
                "query": {
                    "filtered": {
                        "filter": {
                            "range": {
                                "_ts": {"gte": start_ts, "lte": end_ts}
                            }
                        }
                    }
                }
            })

    def index(self, action, meta_action, doc_source=None, update_spec=None):
        with self.lock:
            self.BulkBuffer.add_upsert(action,meta_action,doc_source,update_spec)

        # Divide by two to account for meta actions
        if len(self.BulkBuffer.action_buffer) / 2 >= self.chunk_size or self.auto_commit_interval == 0:
            self.commit()

    def commit(self):
        """Send bulk requests and clear buffer"""
        with self.lock:
            try:
                action_buffer = self.BulkBuffer.get_buffer()
                if action_buffer:
                    successes, errors = bulk(self.elastic, action_buffer)
            except Exception as e:
                # Handle errors from bulk indexing request
                raise
            
        retry_until_ok(self.elastic.indices.refresh, index="")

    def run_auto_commit(self):
        """Periodically commit to the Elastic server."""
        self.commit()
        if self.auto_commit_interval not in [None, 0]:
            Timer(self.auto_commit_interval, self.run_auto_commit).start()

    @wrap_exceptions
    def get_last_doc(self):
        """Get the most recently modified document from Elasticsearch.

        This method is used to help define a time window within which documents
        may be in conflict after a MongoDB rollback.
        """
        try:
            result = self.elastic.search(
                index=self.meta_index_name,
                body={
                    "query": {"match_all": {}},
                    "sort": [{"_ts": "desc"}],
                },
                size=1
            )["hits"]["hits"]
            for r in result:
                r['_source']['_id'] = r['_id']
                return r['_source']
        except es_exceptions.RequestError:
            # no documents so ES returns 400 because of undefined _ts mapping
            return None
        
class BulkBuffer():
    def __init__(self,docman):
        
        # Parent object
        self.docman = docman
        
        # Action buffer for bulk indexing
        self.action_buffer = []
        self.doc_to_get = []
        
        # Dictionary of sources
        # Format: {"_index": {"_type": {"_id": {"_source": actual_source}}}}
        self.sources = {}
        
    def add_upsert(self, action, meta_action, doc_source, update_spec):
        """
        Function which stores sources for "insert" actions
        and decide if for "update" action has to add docs to 
        get source buffer
        """
        
        # in case that source is empty, it means that we need
        # to get that later with multi get API
        if update_spec:
            self.bulk_index(action, meta_action)
            
            # -1 -> to get latest index number
            # -1 -> to get action instead of meta_action
            self.add_doc_to_get(action,update_spec,len(self.action_buffer)-2)
        else:
            # for delete action there will be no doc_source
            if doc_source:
                self.add_to_sources(action,doc_source)
            self.bulk_index(action, meta_action)
            
    def add_doc_to_get(self,action,update_spec,action_buffer_index):
        """Prepare document for MGET elasticsearch API"""
        doc = {'_index' : action['_index'],
               '_type' : action['_type'],
               '_id' : action['_id']}
        self.doc_to_get.append((doc,update_spec,action_buffer_index))
        
    def get_docs_sources(self):
        """Get document sources using MGET elasticsearch API"""
        docs = [doc for doc,_,_ in self.doc_to_get]
        
        retry_until_ok(self.docman.elastic.indices.refresh, index="")
        documents = self.docman.elastic.mget(body={'docs': docs})
        return documents
    
    @wrap_exceptions
    def update_sources(self):
        """Update local sources based on response from elasticsearch"""
        documents = self.get_docs_sources()
        
        for index,each_doc in enumerate(documents['docs']):
            if each_doc['found']:
                _,update_spec,action_buffer_index = self.doc_to_get[index]
                
                # maybe source already has been taken from elasticsearch
                # and updated. In that case get source from sources
                source = self.get_from_sources(each_doc['_index'], each_doc['_type'], each_doc['_id'])
                if not source:
                    source = each_doc['_source']
                updated = self.docman.apply_update(source, update_spec)
                
                #Remove _id field from source
                if '_id' in updated: del updated['_id']
                # Everytime update source to keep it up-to-date
                self.add_to_sources(each_doc,updated)
                
                self.action_buffer[action_buffer_index]['_source'] = Formatter.format_document(updated)
            else:
                # Document not found in elasticsearch,
                # Seems like something went wrong during replication
                # or you tried to update document which while was inserting
                # didn't contain any field mapped in mongo-connector configuration
                self.doc_to_get = []
                raise errors.OperationFailed(
                    "mGET: Document id: {} has not been found".format(each_doc['_id']))
        self.doc_to_get = []
    
    def add_to_sources(self,action,doc_source):
        """Store sources locally"""
        index = action['_index']
        doc_type = action['_type']
        document_id = action['_id']
        new_source = doc_source
        current_type = self.sources.get(index, {}).get(doc_type, {})
        if current_type:
            self.sources[index][doc_type][document_id] = new_source
        else:
            self.sources.setdefault(index,{})[doc_type] = {document_id : new_source}
    
    def get_from_sources(self,index,doc_type,document_id):
        """Get source stored locally"""
        return self.sources.get(index, {}).get(doc_type, {}).get(document_id,{})

    def bulk_index(self, action, meta_action):
        self.action_buffer.append(action)
        self.action_buffer.append(meta_action)
            
    def get_buffer(self):
        """Get buffer which needs to be bulked to elasticsearch"""
        
        # Get sources for documents which are in elasticsearch
        # and they are not in local buffer
        if self.doc_to_get: self.update_sources()
        
        ES_buffer = deepcopy(self.action_buffer)
        self.action_buffer = []
        self.sources = {}
        return ES_buffer
