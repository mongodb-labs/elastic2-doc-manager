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

"""Unit tests for the Elastic2 DocManager."""
import base64
import sys
import time

sys.path[0:0] = [""]

from mongo_connector import errors
from mongo_connector.command_helper import CommandHelper
from mongo_connector.doc_managers.elastic2_doc_manager import (
    DocManager, _HAS_AWS, convert_aws_args, create_aws_auth)
from mongo_connector.test_utils import MockGridFSFile, TESTARGS
from mongo_connector.util import retry_until_ok

from tests import unittest, elastic_pair
from tests.test_elastic2 import ElasticsearchTestCase


class TestElasticDocManager(ElasticsearchTestCase):
    """Unit tests for the Elastic DocManager."""

    def test_update(self):
        """Test the update method using locally stored source"""

        # If testing with BulkBuffer, auto_commit_interval
        # needs to be None to not clear locally stored sources
        self.elastic_doc.auto_commit_interval = None

        doc_id = 1
        doc = {"_id": doc_id, "a": 1, "b": 2}
        self.elastic_doc.upsert(doc, *TESTARGS)

        # $set only
        update_spec = {"$set": {"a": 1, "b": 2}}
        doc = self.elastic_doc.update(doc_id, update_spec, *TESTARGS)

        self.assertEqual(doc, {"_id": '1', "a": 1, "b": 2})
        # $unset only
        update_spec = {"$unset": {"a": True}}
        doc = self.elastic_doc.update(doc_id, update_spec, *TESTARGS)
        self.assertEqual(doc, {"_id": '1', "b": 2})
        # mixed $set/$unset
        update_spec = {"$unset": {"b": True}, "$set": {"c": 3}}
        doc = self.elastic_doc.update(doc_id, update_spec, *TESTARGS)
        self.assertEqual(doc, {"_id": '1', "c": 3})

        # Commit doc to Elasticsearch and get it from there
        # to test if BulkBuffer works fine
        self.elastic_doc.commit()
        res = self._search()
        self.assertEqual(doc, next(res))

        # set auto_commit_interval back to 0
        self.elastic_doc.auto_commit_interval = 0

    def test_upsert(self):
        """Test the upsert method."""
        docc = {'_id': '1', 'name': 'John'}
        self.elastic_doc.upsert(docc, *TESTARGS)
        res = self.elastic_conn.search(
            index="test", doc_type='test',
            body={"query": {"match_all": {}}}
        )["hits"]["hits"]
        for doc in res:
            self.assertEqual(doc['_id'], '1')
            self.assertEqual(doc['_source']['name'], 'John')

    def test_update_using_ES(self):
        """
        Test the update method and getting sources for update
        for Elasticsearch
        """

        # If testing with BulkBuffer, auto_commit_interval
        # needs to be None to not clear locally stored sources
        self.elastic_doc.auto_commit_interval = None

        doc_id = 1
        doc = {"_id": doc_id, "a": 1, "b": 2}
        self.elastic_doc.upsert(doc, *TESTARGS)
        self.elastic_doc.commit()

        update_spec = {"$set": {"a": 1, "b": 2}}
        self.elastic_doc.update(doc_id, update_spec, *TESTARGS)

        update_spec = {"$set": {"a": 10, "b": 20}}
        self.elastic_doc.update(doc_id, update_spec, *TESTARGS)

        update_spec = {"$set": {"a": 100, "b": 200}}
        self.elastic_doc.update(doc_id, update_spec, *TESTARGS)

        # Commit doc to Elasticsearch and get it from there
        # to test if BulkBuffer works fine
        doc["a"] = 100
        doc["b"] = 200
        self.elastic_doc.commit()
        res = self._search()
        self.assertEqual(doc, next(res))

        # set auto_commit_interval back to 0
        self.elastic_doc.auto_commit_interval = 0

    def test_upsert(self):
        """Test the upsert method."""
        docc = {'_id': '1', 'name': 'John'}
        self.elastic_doc.upsert(docc, *TESTARGS)
        res = self.elastic_conn.search(
            index="test", doc_type='test',
            body={"query": {"match_all": {}}}
        )["hits"]["hits"]
        for doc in res:
            self.assertEqual(doc['_id'], '1')
            self.assertEqual(doc['_source']['name'], 'John')

    def test_upsert_with_updates(self):
        """Test the upsert method with multi updates
        and clearing buffer (commit) after each update."""

        doc_id = 1
        docc = {'_id': doc_id, 'name': 'John'}
        self.elastic_doc.upsert(docc, *TESTARGS)

        update_spec = {'$set': {'a': 1, 'b': 2}}
        self.elastic_doc.update(doc_id, update_spec, *TESTARGS)

        update_spec = {'$set': {'a': 2, 'b': 3, 'c': ['test']}}
        self.elastic_doc.update(doc_id, update_spec, *TESTARGS)

        update_spec = {'$set': {'c': ['test', 'test2']}}
        self.elastic_doc.update(doc_id, update_spec, *TESTARGS)

        res = self.elastic_conn.search(
            index="test", doc_type='test',
            body={"query": {"match_all": {}}}
        )["hits"]["hits"]

        for doc in res:
            self.assertEqual(doc['_id'], '1')
            self.assertEqual(doc['_source']['name'], 'John')
            self.assertEqual(doc['_source']['a'], 2)
            self.assertEqual(doc['_source']['b'], 3)
            self.assertEqual(doc['_source']['c'], ["test", "test2"])

    def test_bulk_upsert(self):
        """Test the bulk_upsert method."""
        self.elastic_doc.bulk_upsert([], *TESTARGS)

        docs = ({"_id": i} for i in range(1000))
        self.elastic_doc.bulk_upsert(docs, *TESTARGS)
        self.elastic_doc.commit()
        returned_ids = sorted(int(doc["_id"]) for doc in self._search())
        self.assertEqual(self._count(), 1000)
        self.assertEqual(len(returned_ids), 1000)
        for i, r in enumerate(returned_ids):
            self.assertEqual(r, i)

        docs = ({"_id": i, "weight": 2 * i} for i in range(1000))
        self.elastic_doc.bulk_upsert(docs, *TESTARGS)

        returned_ids = sorted(
            int(doc["weight"]) for doc in self._search())
        self.assertEqual(len(returned_ids), 1000)
        for i, r in enumerate(returned_ids):
            self.assertEqual(r, 2 * i)

    def test_remove(self):
        """Test the remove method."""
        docc = {'_id': '1', 'name': 'John'}
        self.elastic_doc.upsert(docc, *TESTARGS)
        res = self.elastic_conn.search(
            index="test", doc_type='test',
            body={"query": {"match_all": {}}}
        )["hits"]["hits"]
        res = [x["_source"] for x in res]
        self.assertEqual(len(res), 1)

        self.elastic_doc.remove(docc['_id'], *TESTARGS)
        res = self.elastic_conn.search(
            index="test", doc_type='test',
            body={"query": {"match_all": {}}}
        )["hits"]["hits"]
        res = [x["_source"] for x in res]
        self.assertEqual(len(res), 0)

    def test_insert_file(self):
        """Ensure we can properly insert a file into ElasticSearch
        """
        test_data = ' '.join(str(x) for x in range(100000)).encode('utf8')
        docc = {
            '_id': 'test_id',
            'filename': 'test_filename',
            'upload_date': 5,
            'md5': 'test_md5'
        }
        self.elastic_doc.insert_file(
            MockGridFSFile(docc, test_data), *TESTARGS)
        res = self._search()
        for doc in res:
            self.assertEqual(doc['_id'], docc['_id'])
            self.assertEqual(doc['filename'], docc['filename'])
            self.assertEqual(base64.b64decode(doc['content']),
                             test_data.strip())

    def test_remove_file(self):
        test_data = b'hello world'
        docc = {
            '_id': 'test_id',
            '_ts': 10,
            'ns': 'test.test',
            'filename': 'test_filename',
            'upload_date': 5,
            'md5': 'test_md5'
        }

        self.elastic_doc.insert_file(
            MockGridFSFile(docc, test_data), *TESTARGS)
        res = list(self._search())
        self.assertEqual(len(res), 1)

        self.elastic_doc.remove('test_id', *TESTARGS)
        res = list(self._search())
        self.assertEqual(len(res), 0)

    def test_search(self):
        """Test the search method.

        Make sure we can retrieve documents last modified within a time range.
        """
        docc = {'_id': '1', 'name': 'John'}
        self.elastic_doc.upsert(docc, 'test.test', 5767301236327972865)
        docc2 = {'_id': '2', 'name': 'John Paul'}
        self.elastic_doc.upsert(docc2, 'test.test', 5767301236327972866)
        docc3 = {'_id': '3', 'name': 'Paul'}
        self.elastic_doc.upsert(docc3, 'test.test', 5767301236327972870)
        search = list(self.elastic_doc.search(5767301236327972865,
                                              5767301236327972866))
        self.assertEqual(len(search), 2)
        result_ids = [result.get("_id") for result in search]
        self.assertIn('1', result_ids)
        self.assertIn('2', result_ids)

    def disable_auto_refresh(self):
        """Disable default 1 second auto refresh in Elasticsearch.

        https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html
        """
        self.elastic_conn.indices.put_settings(
            index='test', body={'index': {'refresh_interval': '-1'}})

    def enable_auto_refresh(self):
        """Enable default 1 second auto refresh in Elasticsearch.
        """
        self.elastic_conn.indices.put_settings(
            index='test', body={'index': {'refresh_interval': '1s'}})

    def test_elastic_commit(self):
        """Test the auto_commit_interval attribute."""
        self.disable_auto_refresh()
        doc = {'_id': '3', 'name': 'Waldo'}

        # test cases:
        # None = no autocommit
        # 0 = commit immediately
        # x > 0 = commit within x seconds
        for commit_interval in [None, 0, 2, 8]:
            docman = DocManager(elastic_pair,
                                auto_commit_interval=commit_interval)
            docman.upsert(doc, *TESTARGS)
            if commit_interval > 0:
                # Allow just a little extra time
                time.sleep(commit_interval + 2)
            results = list(self._search())
            if commit_interval is None:
                self.assertEqual(len(results), 0,
                                 "should not commit document with "
                                 "auto_commit_interval = None")
            else:
                self.assertEqual(len(results), 1,
                                 "should commit document with "
                                 "auto_commit_interval = %s" % (
                                 commit_interval,))
                self.assertEqual(results[0]["name"], "Waldo")
            docman.stop()
            self._remove()
            retry_until_ok(self.elastic_conn.indices.refresh, index="")
        self.enable_auto_refresh()

    def test_auto_send_interval(self):
        """Test the auto_send_interval

        auto_send_interval should control the amount of time to wait before
        sending (but not committing) buffered operations.
        """
        self.disable_auto_refresh()
        doc = {'_id': '3', 'name': 'Waldo'}

        # test cases:
        # None, 0 = no auto send
        # x > 0 = send buffered operations within x seconds
        for send_interval in [None, 0, 3, 8]:
            docman = DocManager(elastic_pair,
                                autoSendInterval=send_interval,
                                auto_commit_interval=None)
            docman.upsert(doc, *TESTARGS)
            if send_interval > 0:
                # Allow just a little extra time
                time.sleep(send_interval + 2)
            results = list(self._search())
            self.assertEqual(
                len(results), 0,
                "documents should not be commited with "
                "auto_commit_interval=None and auto_commit_interval=%s" % (
                    send_interval,))
            # Commit the possibly sent changes and search again
            retry_until_ok(self.elastic_conn.indices.refresh, index="")
            results = list(self._search())
            if send_interval <= 0:
                self.assertEqual(
                    len(results), 0,
                    "should not send document with auto_send_interval=%s" % (
                        send_interval,))
            else:
                self.assertEqual(
                    len(results), 1,
                    "should send document with auto_send_interval=%s" % (
                        send_interval,))
                self.assertEqual(results[0]["name"], "Waldo")
            docman.stop()
            self._remove()
            retry_until_ok(self.elastic_conn.indices.refresh, index="")
        self.enable_auto_refresh()

    def test_get_last_doc(self):
        """Test the get_last_doc method.

        Make sure we can retrieve the document most recently modified from ES.
        """
        base = self.elastic_doc.get_last_doc()
        ts = base.get("_ts", 0) if base else 0
        docc = {'_id': '4', 'name': 'Hare'}
        self.elastic_doc.upsert(docc, 'test.test', ts + 3)
        docc = {'_id': '5', 'name': 'Tortoise'}
        self.elastic_doc.upsert(docc, 'test.test', ts + 2)
        docc = {'_id': '6', 'name': 'Mr T.'}
        self.elastic_doc.upsert(docc, 'test.test', ts + 1)

        self.assertEqual(
            self.elastic_doc.elastic.count(index="test")['count'], 3)
        doc = self.elastic_doc.get_last_doc()
        self.assertEqual(doc['_id'], '4')

        docc = {'_id': '6', 'name': 'HareTwin'}
        self.elastic_doc.upsert(docc, 'test.test', ts + 4)
        doc = self.elastic_doc.get_last_doc()
        self.assertEqual(doc['_id'], '6')
        self.assertEqual(
            self.elastic_doc.elastic.count(index="test")['count'], 3)

    def test_commands(self):
        cmd_args = ('test.$cmd', 1)
        self.elastic_doc.command_helper = CommandHelper()

        self.elastic_doc.handle_command({'create': 'test2'}, *cmd_args)
        retry_until_ok(self.elastic_conn.indices.refresh, index="")
        self.assertIn('test2', self._mappings('test'))

        docs = [
            {"_id": 0, "name": "ted"},
            {"_id": 1, "name": "marsha"},
            {"_id": 2, "name": "nikolas"}
        ]
        self.elastic_doc.upsert(docs[0], 'test.test2', 1)
        self.elastic_doc.upsert(docs[1], 'test.test2', 1)
        self.elastic_doc.upsert(docs[2], 'test.test2', 1)

        # Commit upserted docs as they are in buffer
        self.elastic_doc.commit()

        res = list(self.elastic_doc._stream_search(
            index="test", doc_type='test2',
            body={"query": {"match_all": {}}}
        ))
        for d in docs:
            self.assertTrue(d in res)

        self.elastic_doc.handle_command({'drop': 'test2'}, *cmd_args)
        retry_until_ok(self.elastic_conn.indices.refresh, index="")
        res = list(self.elastic_doc._stream_search(
            index="test", doc_type='test2',
            body={"query": {"match_all": {}}})
        )
        self.assertEqual(0, len(res))

        self.elastic_doc.handle_command({'create': 'test2'}, *cmd_args)
        self.elastic_doc.handle_command({'create': 'test3'}, *cmd_args)
        retry_until_ok(self.elastic_conn.indices.refresh, index="")
        self.elastic_doc.handle_command({'dropDatabase': 1}, *cmd_args)
        retry_until_ok(self.elastic_conn.indices.refresh, index="")
        self.assertNotIn('test', self._indices())
        self.assertNotIn('test2', self._mappings())
        self.assertNotIn('test3', self._mappings())

    def buffer_and_drop(self):
        """Insert document and drop collection while doc is in buffer"""

        self.elastic_doc.command_helper = CommandHelper()

        self.elastic_doc.auto_commit_interval = None
        index = "test3"
        doc_type = "foo"
        cmd_args = ('%s.%s' % (index, doc_type), 1)

        doc_id = 1
        doc = {"_id": doc_id, "name": "bar"}
        self.elastic_doc.upsert(doc, *cmd_args)

        self.elastic_doc.handle_command({'drop': doc_type}, *cmd_args)
        retry_until_ok(self.elastic_conn.indices.refresh, index="")

        # Commit should be called before command has been handled
        # Which means that buffer should be empty
        self.assertFalse(self.elastic_doc.BulkBuffer.get_buffer())

        # After drop, below search should return no results
        res = list(self.elastic_doc._stream_search(
            index=index, doc_type=doc_type,
            body={"query": {"match_all": {}}})
        )
        self.assertFalse(res)

        # Test dropDatabase as well
        # Firstly add document to database again
        # This time update doc as well
        self.elastic_doc.upsert(doc, *cmd_args)
        update_spec = {"$set": {"name": "foo2"}}
        self.elastic_doc.update(doc_id, update_spec, *cmd_args)
        self.elastic_doc.handle_command({'dropDatabase': 1}, *cmd_args)
        retry_until_ok(self.elastic_conn.indices.refresh, index="")
        self.assertFalse(self.elastic_doc.BulkBuffer.get_buffer())
        self.assertNotIn(index, self._mappings())

        # set auto_commit_interval back to 0
        self.elastic_doc.auto_commit_interval = 0


class TestElasticDocManagerAWS(unittest.TestCase):

    @unittest.skipIf(_HAS_AWS, 'Cannot test with AWS extension installed')
    def test_aws_raises_invalid_configuration(self):
        with self.assertRaises(errors.InvalidConfiguration):
            DocManager('notimportant', aws={})

    def test_convert_aws_args(self):
        aws_args = dict(region_name='name',
                        aws_access_key_id='id',
                        aws_secret_access_key='key',
                        aws_session_token='token',
                        profile_name='profile_name')
        self.assertEqual(convert_aws_args(aws_args), aws_args)

    def test_convert_aws_args_raises_invalid_configuration(self):
        with self.assertRaises(errors.InvalidConfiguration):
            convert_aws_args('not_dict')

    def test_convert_aws_args_old_options(self):
        self.assertEqual(
            convert_aws_args(dict(region='name',
                                  access_id='id',
                                  secret_key='key')),
            dict(region_name='name',
                 aws_access_key_id='id',
                 aws_secret_access_key='key'))

    @unittest.skipUnless(_HAS_AWS, 'Cannot test without AWS extension')
    def test_create_aws_auth_raises_invalid_configuration(self):
        with self.assertRaises(errors.InvalidConfiguration):
            create_aws_auth({'unknown_option': ''})


if __name__ == '__main__':
    unittest.main()
