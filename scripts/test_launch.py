import os

import docker
import pytest

from . import launch, util


@pytest.fixture
def client():
    return docker.from_env()


@pytest.fixture(scope='function')
def clean_images():
    images = docker.from_env().images
    before = {x.id for x in images.list()}
    yield
    after = {x.id for x in images.list()}
    map(images.remove, after.difference(before))


@pytest.mark.parametrize('dockerfile', util.find_dockerfiles('.'))
def test_build_image(client, dockerfile):
    dockerfile_path = 'docker/{}'.format(dockerfile)
    launch.build_image(client, dockerfile=dockerfile_path)
    tags = [tag for x in client.images.list() for tag in x.tags]
    expected_tag = '{}/{}:latest'.format(launch.PROJECT_NAME, dockerfile)
    assert expected_tag in tags
