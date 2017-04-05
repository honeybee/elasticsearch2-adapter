<?php

namespace Honeybee\Elasticsearch2\Finder;

use Assert\Assertion;
use Honeybee\Common\Error\RuntimeError;
use Honeybee\Infrastructure\Config\ConfigInterface;
use Honeybee\Infrastructure\DataAccess\Query\AttributeCriteria;
use Honeybee\Infrastructure\DataAccess\Query\Comparison\Equals;
use Honeybee\Infrastructure\DataAccess\Query\Comparison\In;
use Honeybee\Infrastructure\DataAccess\Query\CriteriaContainerInterface;
use Honeybee\Infrastructure\DataAccess\Query\CriteriaInterface;
use Honeybee\Infrastructure\DataAccess\Query\CriteriaList;
use Honeybee\Infrastructure\DataAccess\Query\CriteriaQueryInterface;
use Honeybee\Infrastructure\DataAccess\Query\CustomCriteria;
use Honeybee\Infrastructure\DataAccess\Query\Geometry\Annulus;
use Honeybee\Infrastructure\DataAccess\Query\Geometry\Box;
use Honeybee\Infrastructure\DataAccess\Query\Geometry\Circle;
use Honeybee\Infrastructure\DataAccess\Query\Geometry\Polygon;
use Honeybee\Infrastructure\DataAccess\Query\QueryInterface;
use Honeybee\Infrastructure\DataAccess\Query\QueryTranslationInterface;
use Honeybee\Infrastructure\DataAccess\Query\RangeCriteria;
use Honeybee\Infrastructure\DataAccess\Query\SearchCriteria;
use Honeybee\Infrastructure\DataAccess\Query\SpatialCriteria;

class CriteriaQueryTranslation implements QueryTranslationInterface
{
    const QUERY_FOR_EMPTY = '__empty';

    protected $config;

    public function __construct(ConfigInterface $config)
    {
        $this->config = $config;
    }

    public function translate(QueryInterface $query)
    {
        Assertion::isInstanceOf($query, CriteriaQueryInterface::CLASS);

        $esQuery = [
            'from' => $query->getOffset(),
            'size' => $query->getLimit(),
            'body' => $this->buildBody($query)
        ];

        return $esQuery;
    }

    protected function buildBody(QueryInterface $query)
    {
        $filterCriteriaList = $query->getFilterCriteriaList();
        foreach ($this->config->get('query_filters', []) as $attributePath => $attributeValue) {
            $criteria = new AttributeCriteria($attributePath, new Equals($attributeValue));
            $filterCriteriaList->push($criteria);
        }

        $esFilter = $this->translateFilters($filterCriteriaList);
        $esQuery = $this->translateQueries($query->getSearchCriteriaList());
        if (!empty($esFilter)) {
            $esQuery = [
                'bool' => [
                    'must' => $esQuery,
                    'filter' => $esFilter
                ]
            ];
        }

        return [
            'query' => $esQuery,
            'sort' => $this->buildSort($query)
        ];
    }

    protected function translateQueries(CriteriaList $criteriaList)
    {
        if ($criteriaList->isEmpty()) {
            return ['match_all' => []];
        } else {
            // @todo atm we only support global search on the _all field
            // more complex search query building will follow up
            $searchCriteria = $criteriaList->getFirst();
            if (!$searchCriteria instanceof SearchCriteria) {
                throw new RuntimeError(
                    sprintf('Only instances of %s supported as search-criteria.', SearchCriteria::CLASS)
                );
            }

            $phrase = $searchCriteria->getPhrase();

            if (preg_match('~^suggest:([\.\w]+)=(.+)~', $phrase, $matches)) {
                $suggestFieldParts = [];
                // strip the 'type' portion of the attribute-path, to address props the way ES expects
                foreach (explode('.', $matches[1]) as $i => $fieldPart) {
                    if ($i % 2 === 0) {
                        $suggestFieldParts[] = $fieldPart;
                    }
                }
                $suggestField = implode('.', $suggestFieldParts);
                $suggestField .= '.suggest'; // convention: multi field for suggestions
                $suggestTerm = $matches[2];

                return [
                    'match_phrase_prefix' => [
                        $suggestField => ['query' => $suggestTerm, 'max_expansions' => 15]
                    ]
                ];
            } else {
                return $this->buildSearchQuery($searchCriteria);
            }
        }
    }

    protected function translateFilters(CriteriaList $filterCriteriaList)
    {
        $elasticsearchFilters = [];
        foreach ($filterCriteriaList as $criteria) {
            if ($criteria instanceof CriteriaContainerInterface) {
                $operator = $criteria->getOperator();
                $filters = $this->translateFilters($criteria->getCriteriaList());
                if (!empty($filters)) {
                    if (isset($elasticsearchFilters[$operator])) {
                        $elasticsearchFilters[] = array_merge_recursive(
                            $elasticsearchFilters[$operator],
                            $filters
                        );
                    } else {
                        $elasticsearchFilters[] = $filters;
                    }
                }
            } elseif ($criteria instanceof AttributeCriteria) {
                $elasticsearchFilters[] = $this->buildFilterFor($criteria);
            } elseif ($criteria instanceof RangeCriteria) {
                $elasticsearchFilters[] = $this->buildRangeFilterFor($criteria);
            } elseif ($criteria instanceof SpatialCriteria) {
                $elasticsearchFilters[] = $this->buildSpatialFilterFor($criteria);
            } elseif ($criteria instanceof CustomCriteria) {
                $elasticsearchFilters[] = $criteria->getQueryPart();
            } else {
                throw new RuntimeError(
                    sprintf('Invalid criteria type %s given to %s', get_class($criteria), static::CLASS)
                );
            }
        }

        if (count($elasticsearchFilters)) {
            return [$filterCriteriaList->getOperator() => $elasticsearchFilters];
        } else {
            return [];
        }
    }

    protected function buildFilterFor(CriteriaInterface $criteria)
    {
        $negateFilter = false;
        $attributeValue = $criteria->getComparison()->getComparand();
        $attributePath = $criteria->getAttributePath();

        if (is_array($attributeValue)) {
            $filter = ['terms' => [$attributePath => $attributeValue]];
            if ($criteria->getComparison()->isInverted()) {
                return ['not' => $filter];
            }
            return $filter;
        }

        if ($criteria->getComparison()->isInverted() || strpos($attributeValue, '!') === 0) {
            $negateFilter = true;
            $attributeValue = substr($attributeValue, 0);
        }
        if ($attributeValue === self::QUERY_FOR_EMPTY) {
            $attrFilter = $this->buildMissingFilter($criteria);
        } else {
            $attrFilter = $this->buildTermFilter($criteria);
        }

        return $negateFilter ? $this->negateFilter($attrFilter) : $attrFilter;
    }

    protected function buildRangeFilterFor(CriteriaInterface $criteria)
    {
        $attributePath = $criteria->getAttributePath();

        foreach ($criteria->getItems() as $comparison) {
            $comparand = $comparison->getComparand();
            // format date range queries
            if (!is_numeric($comparand) && $ts = strtotime($comparand)) {
                // @todo support for date ranges beyond unix timestamp range
                $comparand = date('c', $ts);
                $comparisons['format'] = "yyyy-MM-dd'T'HH:mm:ssZ";
            }
            $comparisons[$comparison->getComparator()] = $comparand;
        }

        return [
            'range' => [$attributePath => $comparisons]
        ];
    }

    protected function buildSpatialFilterFor(CriteriaInterface $criteria)
    {
        $attributePath = $criteria->getAttributePath();
        $comparison = $criteria->getComparison();
        $geometry = $comparison->getComparand();

        if ($comparison instanceof In) {
            if ($geometry instanceof Circle) {
                $filter = [
                    'geo_distance' => [
                        'distance' => $geometry->getRadius(),
                        $attributePath => (string)$geometry->getCenter()
                    ]
                ];
            } elseif ($geometry instanceof Annulus) {
                $filter = [
                    'geo_distance_range' => [
                        'from' => $geometry->getInnerRadius(),
                        'to' => $geometry->getOuterRadius(),
                        $attributePath => (string)$geometry->getCenter()
                    ]
                ];
            } elseif ($geometry instanceof Box) {
                $filter = [
                    'geo_bounding_box' => [
                        $attributePath => [
                            'top_left' => (string)$geometry->getTopLeft(),
                            'bottom_right' => (string)$geometry->getBottomRight()
                        ]
                    ]
                ];
            } elseif ($geometry instanceof Polygon) {
                $filter = [
                    'geo_polygon' => [
                        $attributePath => [
                            'points' => array_map('strval', $geometry->toArray())
                        ]
                    ]
                ];
            } else {
                throw new RuntimeError(
                    sprintf('Invalid comparand %s given to %s', get_class($criteria), static::CLASS)
                );
            }
        } else {
            throw new RuntimeError(
                sprintf('Invalid spatial query comparator %s given to %s', get_class($criteria), static::CLASS)
            );
        }

        return $filter;
    }

    protected function buildMissingFilter(CriteriaInterface $criteria)
    {
        return [
            'missing' => [
                'field' => $criteria->getAttributePath(),
                'existence' => true,
                'null_value' => true
            ]
        ];
    }

    protected function buildTermFilter(CriteriaInterface $criteria)
    {
        $attributeValue = $criteria->getComparison()->getComparand();
        if (strpos($attributeValue, '!') === 0) {
            $attributeValue = substr($attributeValue, 1);
        }

        $attributePath = $criteria->getAttributePath();

        $multiFieldMapped_attributes = (array)$this->config->get('multi_fields', []);
        if (in_array($attributePath, $multiFieldMapped_attributes)) {
            $attributePath = $attributePath . '.filter';
        }

        $attrFilter = ['term' => [$attributePath => $attributeValue]];
        /*
        $terms = explode(',', $attributeValue);
        if (count($terms) > 1) {
            $attrFilter = ['terms' => [$attributePath => $terms]];
        } else {
            $attrFilter = ['term' => [$attributePath => $terms[0]]];
        }
        */
        return $attrFilter;
    }

    protected function negateFilter(array $filter)
    {
        return ['not' => $filter];
    }

    protected function buildSort(QueryInterface $query)
    {
        $sorts = [];
        $dynamicMappings = $this->getDynamicMappings();

        foreach ($query->getSortCriteriaList() as $sortCriteria) {
            $attributePath = $sortCriteria->getAttributePath();
            $sort = ['order' => $sortCriteria->getDirection()];
            if (isset($dynamicMappings[$attributePath])) {
                $sort['unmapped_type'] = $dynamicMappings[$attributePath];
            }
            $multiFieldMapped_attributes = (array)$this->config->get('multi_fields', []);
            if (in_array($attributePath, $multiFieldMapped_attributes)) {
                $attributePath = $attributePath . '.sort';
            }
            $sorts[][$attributePath] = $sort;
        }

        return $sorts;
    }

    protected function buildSearchQuery(SearchCriteria $searchCriteria)
    {
        $phrase = $searchCriteria->getPhrase();
        $field = trim($searchCriteria->getAttributePath());
        if (empty($field)) {
            $field = '_all';
        }

        $searchQuerySettings = array_merge(
            [
                'query' => $phrase,
                'type' => 'phrase_prefix',
                // to get more search results you might want to configure e.g.:
                // 'prefix_length' => 4,
                // 'fuzziness' => 'auto',
                // 'max_expansions' => 1000
            ],
            (array)$this->config->get('search_query_settings', [])
        );

        $searchQuery = [
            'match' => [
                $field => $searchQuerySettings
            ]
        ];

        return $searchQuery;
    }

    protected function getDynamicMappings()
    {
        return array_merge(
            (array)$this->config->get('dynamic_mappings', []),
            [
                'identifier' => 'string',
                'referenced_identifier' => 'string',
                'uuid' => 'string',
                'language' => 'string',
                'version' => 'long',
                'revision' => 'long',
                'short_id' => 'long',
                'created_at' => 'date',
                'modified_at' => 'date',
                'workflow_state' => 'string'
            ]
        );
    }
}
