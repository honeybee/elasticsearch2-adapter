<?php

namespace Honeybee\Tests\Elasticsearch2\Fixture\BookSchema\Projection\Author\Embed;

use Honeybee\Projection\EmbeddedEntityType;
use Trellis\Common\Options;
use Trellis\Runtime\Attribute\AttributeInterface;
use Trellis\Runtime\Attribute\Text\TextAttribute;
use Trellis\Runtime\EntityTypeInterface;

class HighlightType extends EmbeddedEntityType
{
    public function __construct(EntityTypeInterface $parent = null, AttributeInterface $parentAttribute = null)
    {
        parent::__construct(
            'Highlight',
            [
                new TextAttribute('title', $this, ['mandatory' => true], $parentAttribute),
                new TextAttribute('description', $this, [], $parentAttribute)
            ],
            new Options,
            $parent,
            $parentAttribute
        );
    }

    public static function getEntityImplementor()
    {
        return Highlight::CLASS;
    }
}
