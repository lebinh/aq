from unittest import TestCase

import boto3
from botocore.exceptions import NoRegionError

from aq import BotoSqliteEngine
from aq.engines import get_resource_model_attributes

import os, tempfile
from nose.tools import eq_

class TestCommandLineArg(TestCase):

    def setUp(self):
        try:
            del os.environ['AWS_PROFILE']
            del os.environ['AWS_DEFAULT_REGION']
            del os.environ['AWS_CONFIG_FILE']
            del os.environ['AWS_SHARED_CREDENTIALS_FILE']
        except:
            pass

        self.credential_file = tempfile.NamedTemporaryFile()
        os.environ['AWS_SHARED_CREDENTIALS_FILE'] = self.credential_file.name
        self.credential_file.write(
            b'[profile_env]\n'
            'region=region-profile-env\n'
            '\n'
            '[profile_arg]\n'
            'region=region-profile-arg\n'
        )
        self.credential_file.flush()

        self.config_file = tempfile.NamedTemporaryFile()
        os.environ['AWS_CONFIG_FILE'] = self.config_file.name
        self.config_file.write(
            b'[default]\n'
            'region=region-config-default\n'
            '\n'
            '[config_env]\n'
            'region=region-config-env\n'
        )
        self.config_file.flush()

    def test_command_line_arg_profile(self):
        os.environ['AWS_PROFILE'] = 'profile_env'
        os.environ['AWS_CONFIG_FILE'] = 'config_env'
        os.environ['AWS_DEFAULT_REGION'] = 'region-env'
        engine = BotoSqliteEngine({ '--profile': 'profile_arg' })

        eq_(engine.boto3_session.profile_name, 'profile_arg')

    def test_command_line_arg_region(self):
        os.environ['AWS_PROFILE'] = 'profile_env'
        os.environ['AWS_CONFIG_FILE'] = 'config_env'
        os.environ['AWS_DEFAULT_REGION'] = 'region-env'
        engine = BotoSqliteEngine({ '--region': 'region-arg' })

        eq_(engine.boto3_session.region_name, 'region-arg')

    def test_command_line_arg_none(self):
        os.environ['AWS_PROFILE'] = 'profile_env'
        os.environ['AWS_CONFIG_FILE'] = 'config_env'
        os.environ['AWS_DEFAULT_REGION'] = 'region-env'
        engine = BotoSqliteEngine({})

        eq_(engine.boto3_session.profile_name, 'profile_env')
        eq_(engine.boto3_session.region_name, 'region-env')

    def test_command_line_arg_and_env_file_none(self):
        del os.environ['AWS_CONFIG_FILE']
        del os.environ['AWS_SHARED_CREDENTIALS_FILE']

        engine = BotoSqliteEngine({})

        eq_(engine.boto3_session.profile_name, 'default')
        eq_(engine.boto3_session.region_name, 'us-east-1')
