<?php

namespace Honeybee\Tests\Elasticsearch2\Finder\DomainEvent;

use Elasticsearch\Client;
use Honeybee\Elasticsearch2\Connector\ElasticsearchConnector;
use Honeybee\Elasticsearch2\Finder\DomainEvent\DomainEventFinder;
use Honeybee\Infrastructure\Config\ArrayConfig;
use Honeybee\Infrastructure\DataAccess\Finder\FinderResult;
use Honeybee\Tests\Elasticsearch2\Fixture\BookSchema\Model\Task\CreateAuthor\AuthorCreatedEvent;
use Honeybee\Tests\Elasticsearch2\TestCase;
use Mockery;
use Psr\Log\NullLogger;

class DomainEventFinderTest extends TestCase
{
    private $mockClient;

    private $mockConnector;

    public function setUp()
    {
        $this->mockClient = Mockery::mock(Client::CLASS);
        $this->mockConnector = Mockery::mock(ElasticsearchConnector::CLASS);
        $this->mockConnector->shouldReceive('getConfig')->twice()->withNoArgs()->andReturn(new ArrayConfig([]));
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);
        $this->mockConnector->shouldReceive('isConnected')->never();
    }

    /**
     * @expectedException Honeybee\Common\Error\RuntimeError
     */
    public function testGetByIdentifierNoHits()
    {
        $this->mockClient->shouldReceive('get')->once()
            ->with(['index' => ['index1'], 'type' => ['type1'], 'id' => 'test_id'])->andReturn([]);

        $domainEventFinder = new DomainEventFinder(
            $this->mockConnector,
            new ArrayConfig(['index' => 'index1', 'type' => 'type1']),
            new NullLogger
        );

        $domainEventFinder->getByIdentifier('test_id');
    }

    /**
     * @expectedException Honeybee\Common\Error\RuntimeError
     */
    public function testGetByIdentifierNoObjectType()
    {
        $this->mockClient->shouldReceive('get')->once()
            ->with(['index' => ['_all'], 'type' => ['type1'], 'id' => 'test_id'])
            ->andReturn(['hits' => ['hits' => [['_source' => []]]]]);

        $domainEventFinder = new DomainEventFinder(
            $this->mockConnector,
            new ArrayConfig(['type' => 'type1']),
            new NullLogger
        );

        $domainEventFinder->getByIdentifier('test_id');
    }

    /**
     * @expectedException Honeybee\Common\Error\RuntimeError
     */
    public function testGetByIdentifierInvalidObjectType()
    {
        $this->mockClient->shouldReceive('get')->once()
            ->with(['index' => ['index1'], 'type' => ['type1'], 'id' => 'test_id'])
            ->andReturn(['hits' => ['hits' => [['_source' => ['@type' => 'stdClass']]]]]);

        $domainEventFinder = new DomainEventFinder(
            $this->mockConnector,
            new ArrayConfig(['index' => 'index1', 'type' => 'type1']),
            new NullLogger
        );

        $domainEventFinder->getByIdentifier('test_id');
    }

    public function testGetByIdentifier()
    {
        $source = [
            '@type' => '\Honeybee\Tests\Elasticsearch2\Fixture\BookSchema\Model\Task\CreateAuthor\AuthorCreatedEvent',
            'uuid' => 'c4b4192c-a1e5-41f0-8a8e-253b78474ae5',
            'aggregate_root_type' => 'honeybee_cmf.fixtures.author',
            'aggregate_root_identifier' => 'honeybee_cmf.fixtures.author-c4b4192c-a1e5-41f0-8a8e-253b78474ae5-de_DE-1',
            'seq_number' => 1,
            'iso_date' => '2017-04-04T22:06:44.317148+00:00',
            'data' => [
                'identifier' => 'honeybee_cmf.fixtures.author-c4b4192c-a1e5-41f0-8a8e-253b78474ae5-de_DE-1',
                'uuid' => 'c4b4192c-a1e5-41f0-8a8e-253b78474ae5',
                'language' => 'de_DE',
                'version' => 1,
                'workflow_state' => 'test'
            ]
        ];

        $this->mockClient->shouldReceive('get')->once()
            ->with(['index' => ['_all'], 'type' => ['type1', 'type2'], 'id' => 'test_id'])
            ->andReturn(['hits' => ['hits' => [['_source' => $source]]]]);

        $domainEventFinder = new DomainEventFinder(
            $this->mockConnector,
            new ArrayConfig(['index' => '_all', 'type' => 'type1,type2']),
            new NullLogger
        );

        $expectedResult = new FinderResult([new AuthorCreatedEvent($source)], 1);
        $finderResult = $domainEventFinder->getByIdentifier('test_id');

        $this->assertEquals($expectedResult, $finderResult);
    }
}
