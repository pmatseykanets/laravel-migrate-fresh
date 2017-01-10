<?php

namespace Spatie\MigrateFresh\TableDroppers;

use DB;
use Illuminate\Support\Collection;

class Pgsql implements TableDropper
{
    public function dropAllTables()
    {
        $schema = $this->getSchema();

        $this->dropViews($this->getViews($schema));
        $this->dropTables($this->getTables($schema));
        $this->dropFunctions($this->getFunctions($schema));
        $this->dropSequences($this->getSequences($schema));
        $this->dropDomains($this->getDomains($schema));
        $this->dropCustomTypes($this->getCustomTypes($schema));
    }

    /**
     * Drop all custom types in the schema.
     *
     * @param \Illuminate\Support\Collection $types
     */
    public function dropCustomTypes($types)
    {
        DB::statement("DROP TYPE IF EXISTS {$types->implode(',')} CASCADE");
    }

    /**
     * Get a list of all custom types in the schema.
     *
     * @param $schema
     * @return \Illuminate\Support\Collection
     */
    public function getCustomTypes($schema)
    {
        // For some reason imformation_schema.user_defined_types doesn't return the types
        // See http://stackoverflow.com/questions/3660787/how-to-list-custom-types-using-postgres-information-schema/3703727#3703727
        $sql = <<<'SQL'
SELECT t.typname AS object_name 
  FROM pg_type t LEFT JOIN pg_catalog.pg_namespace n 
    ON n.oid = t.typnamespace 
 WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) 
   AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)
   AND t.typtype NOT IN('d')
   AND n.nspname = ?
SQL;
        return $this->select($sql, $schema);
    }

    /**
     * Drop all domains in the schema.
     *
     * @param \Illuminate\Support\Collection $domains
     */
    public function dropDomains($domains)
    {
        DB::statement("DROP DOMAIN IF EXISTS {$domains->implode(',')} CASCADE");
    }

    /**
     * Get a list of all domains in the schema.
     *
     * @param $schema
     * @return \Illuminate\Support\Collection
     */
    public function getDomains($schema)
    {
        $sql = <<<'SQL'
SELECT domain_name AS object_name
  FROM information_schema.domains 
 WHERE domain_schema = ?
SQL;
        return $this->select($sql, $schema);
    }

    /**
     * Drop all functions in the schema.
     *
     * @param \Illuminate\Support\Collection $functions
     */
    public function dropFunctions($functions)
    {
        $functions->each(function ($function) {
            DB::statement("DROP FUNCTION IF EXISTS $function CASCADE");
        });
    }

    /**
     * Get a list of all functions in the schema.
     *
     * @param $schema
     * @return \Illuminate\Support\Collection
     */
    public function getFunctions($schema)
    {
        $sql = <<<'SQL'
SELECT proname || '(' || oidvectortypes(proargtypes) || ')' AS object_name
  FROM pg_proc INNER JOIN pg_namespace ns 
    ON pg_proc.pronamespace = ns.oid
 WHERE ns.nspname = ?  
 ORDER BY proname;
SQL;

        return $this->select($sql, $schema);
    }

    /**
     * Drop all sequences in the schema.
     *
     * @param \Illuminate\Support\Collection $sequences
     */
    public function dropSequences(Collection $sequences)
    {
        if ($sequences->isEmpty()) {
            return;
        }

        DB::statement("DROP SEQUENCE IF EXISTS {$sequences->implode(',')} CASCADE");
    }

    /**
     * Get a list of all sequences in the schema.
     *
     * @param $schema
     * @return \Illuminate\Support\Collection
     */
    public function getSequences($schema)
    {
        $sql = <<<'SQL'
SELECT sequence_name AS object_name
  FROM information_schema.sequences 
 WHERE sequence_schema = ?
SQL;
        return $this->select($sql, $schema);
    }

    /**
     * Drop tables.
     *
     * @param \Illuminate\Support\Collection $tables
     */
    protected function dropTables(Collection $tables)
    {
        if ($tables->isEmpty()) {
            return;
        }

        DB::statement("DROP TABLE IF EXISTS {$tables->implode(',')} CASCADE");
    }

    /**
     * Get a list of all tables in the schema.
     *
     * @param $schema
     * @return \Illuminate\Support\Collection
     */
    protected function getTables($schema)
    {
        $sql = <<<'SQL'
SELECT tablename AS object_name
  FROM pg_catalog.pg_tables 
 WHERE schemaname = ?
SQL;

        return $this->select($sql, $schema);
    }

    /**
     * Drop all views in the schema.
     *
     * @param \Illuminate\Support\Collection $views
     */
    public function dropViews($views)
    {
        DB::statement("DROP VIEW IF EXISTS {$views->implode(',')} CASCADE");
    }

    /**
     * Get a list of all views in the schema.
     *
     * @param $schema
     * @return \Illuminate\Support\Collection
     */
    public function getViews($schema)
    {
        $sql = <<<'SQL'
SELECT table_name AS object_name
  FROM information_schema.views 
 WHERE table_schema = ?
SQL;
        return $this->select($sql, $schema);
    }

    /**
     * Get schema name for the connection.
     *
     * @return string
     */
    protected function getSchema()
    {
        return DB::getConfig('schema');
    }

    /**
     * Execute the query and returns the list of values.
     *
     * @param string $sql
     * @param array $bindings
     * @param string $column
     * @return \Illuminate\Support\Collection
     */
    protected function select($sql, $bindings = [], $column = 'object_name')
    {
        return collect(
            DB::select($sql, collect($bindings)->all())
        )->pluck($column);
    }
}
