<?php

namespace Honeybee\Tests\Elasticsearch2\Storage\StructureVersionList;

use Honeybee\Elasticsearch2\Storage\StructureVersionList\StructureVersionListWriter;
use Honeybee\Infrastructure\Config\ArrayConfig;
use Honeybee\Infrastructure\DataAccess\Connector\ConnectorInterface;
use Honeybee\Infrastructure\Migration\StructureVersionList;
use Honeybee\Tests\Elasticsearch2\TestCase;
use Mockery;
use Psr\Log\NullLogger;

class StructureVersionListWriterTest extends TestCase
{
    private $mockConnector;

    public function setUp()
    {
        $this->mockConnector = Mockery::mock(ConnectorInterface::CLASS);
    }

    /**
     * @expectedException Assert\InvalidArgumentException
     */
    public function testWriteInvalidVersionList()
    {
        $versionListWriter = new StructureVersionListWriter($this->mockConnector, new ArrayConfig([]), new NullLogger);
        $versionListWriter->write('');
    } //@codeCoverageIgnore

    public function testWrite()
    {
        $versionListWriter = Mockery::mock(
            StructureVersionListWriter::CLASS.'[writeData]',
            [
                $this->mockConnector,
                new ArrayConfig([]),
                new NullLogger
            ]
        )->shouldAllowMockingProtectedMethods('writeData');
        $versionListWriter->shouldReceive('writeData')->once()->with(
            'test_id',
            [
                'identifier' => 'test_id',
                'versions' => ['version' => 'value']
            ]
        )->andReturnNull();
        $mockVersionList = Mockery::mock(StructureVersionList::CLASS);
        $mockVersionList->shouldReceive('getIdentifier')->once()->withNoArgs()->andReturn('test_id');
        $mockVersionList->shouldReceive('toArray')->once()->withNoArgs()->andReturn(['version' => 'value']);

        $versionListWriter->write($mockVersionList);
    }
}
