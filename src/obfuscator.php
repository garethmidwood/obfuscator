<?php

use Ifsnop\Mysqldump as IMysqldump;

Class Obfuscator
{
    /**
     * Constructor
     * @param array $config 
     * @param array $sources
     * @return void
     */
    public function __construct(
        array $config,
        array $sources
    ) {
        $this->config = $config;
        $this->sources = $sources;
    }

    public function run() 
    {
        $this->processSources();
    }

    private function processSources()
    {
        foreach($this->sources as $source) {
            $source->processObfuscation();
        }
    }
}






return;

define('STORAGE_PARTS_DIR', STORAGE_DIR . 'parts' . DIRECTORY_SEPARATOR);
define('CLEANSED_DB_FILE', STORAGE_DIR . 'clean.sql');
define('MANIFEST_FILE', STORAGE_DIR . 'manifest.yml');
define('SQL_STRUCTURE_FILE', '03_structure_obfuscated.sql');
define('SQL_DATA_FILE', '04_data_obfuscated.sql');








function obfuscateField(mysqli $dbConnection, string $tableName, string $obfuscationType, array $fields, bool $dryRun = false) {
    $fieldUpdates = [];

    foreach($fields as $field) {
        switch($obfuscationType) {
            case 'email':
                $fieldUpdates[] = "`$field` = concat(LEFT(UUID(), 8), '@example.com')";
                break;
            case 'date':
                $fieldUpdates[] = "`$field` = CURDATE()";
                break;
            case 'string':
                $fieldUpdates[] = "`$field` = LEFT(UUID(), 10)";
                break;
            case 'float':
                $fieldUpdates[] = "`$field` = '1.0'";
                break;
            case 'bigint':
                $fieldUpdates[] = "`$field` = '12345'";
                break;
            case 'int':
                $fieldUpdates[] = "`$field` = '10'";
                break;
            case 'phone':
                $fieldUpdates[] = "`$field` = '01234567890'";
                break;
            default:
                progressMessage("✓ Unrecognised field type $obfuscationType for `$field` - skipping");
                break;
        }
    }

    $query = 'UPDATE ' . $tableName . ' set ' . implode(',', $fieldUpdates) . ' WHERE 1=1';

    if ($dryRun) {
        progressMessage("✓ DRY RUN: $query");
    } else {
        while ($dbConnection->next_result()) {;} // flush multi_queries

        if (!$dbConnection->multi_query($query)) {
            throw new \Exception('DB Error message: ' . $dbConnection->error . PHP_EOL . 'Query with error: ' . $query);
        }
    }

    progressMessage("✓ Obfuscated $obfuscationType fields in $tableName");
}

echo PHP_EOL;
exit;
