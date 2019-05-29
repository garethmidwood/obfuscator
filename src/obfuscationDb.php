<?php


abstract class obfuscationDb
{
    abstract public function run(string $sql);

    abstract public function selectDb(string $dbname);

    abstract public function dumpDbData(string $dbName, string $outFile);
}
