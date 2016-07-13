from unittest import TestCase

import boto3
from botocore.exceptions import NoRegionError

from aq import BotoSqliteEngine
from aq.engines import get_resource_model_attributes


class TestBotoEngine(TestCase):
    engine = BotoSqliteEngine({})

    def test_is_attached_region(self):
        # main is always attached
        assert self.engine.is_attached_region('main')
        assert not self.engine.is_attached_region('foobar')

    def test_attach_region(self):
        assert not self.engine.is_attached_region('us_west_1')
        self.engine.attach_region('us_west_1')
        assert self.engine.is_attached_region('us_west_1')
        self.engine.db.execute('DETACH DATABASE us_west_1')

    def test_get_resource_model_attributes(self):
        try:
            resource = boto3.resource('ec2')
        except NoRegionError:
            # skip for environment that doesn't have boto config like CI
            pass
        else:
            collection = resource.instances.all()
            attributes = get_resource_model_attributes(resource, collection)
            assert attributes
            assert 'instance_id' in attributes
            assert 'image_id' in attributes
