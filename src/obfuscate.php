<?php

include_once('./vendor/autoload.php');

use Symfony\Component\Yaml\Yaml;
use Ifsnop\Mysqldump as IMysqldump;

define('STORAGE_DIR', sys_get_temp_dir() . DIRECTORY_SEPARATOR);
define('STORAGE_PARTS_DIR', STORAGE_DIR . 'parts' . DIRECTORY_SEPARATOR);
define('CLEANSED_DB_FILE', STORAGE_DIR . 'clean.sql');
define('MANIFEST_FILE', STORAGE_DIR . 'manifest.yml');
define('SQL_STRUCTURE_FILE', '03_structure_obfuscated.sql');
define('SQL_DATA_FILE', '04_data_obfuscated.sql');

// TODO: Move this into a config class that validates itself
$config = Yaml::parseFile('obfuscate.yml');

if (!isset($config['source'])) {
    exit('source node is required');
}

if (!is_array($config['source'])) {
    exit('source node must be an array');
}

if (!isset($config['database'])) {
    exit('database node is required');
}

if (!isset($config['database']['user'])) {
    exit('database user node is required');
}

if (!isset($config['database']['password'])) {
    exit('database password node is required');
}

if (!isset($config['database']['host'])) {
    exit('database host node is required');
}

if (!isset($config['database']['port'])) {
    exit('database port node is required');
}

if (isset($config['dryrun']) && $config['dryrun'] == true) {
    progressMessage('========================');
    progressMessage('======== DRY RUN =======');
    progressMessage('========================');
    define('DRY_RUN', true);
} else {
    define('DRY_RUN', false);
}

$dbConnection = new mysqli(
        $config['database']['host'] . ':' . $config['database']['port'],
        $config['database']['user'],
        $config['database']['password']
    );

if ($dbConnection->connect_errno) {
    errorMessage('Database connection failed: ' . $dbConnection->connect_error, true);
}

if (!$dbConnection->multi_query('SET @@global.max_allowed_packet = 524288000')) {
    errorMessage('Could not set max_allowed_packet on MySQL server: ' . $dbConnection->error, true);
}

progressMessage('✓ Set max_allowed_packet');

// TODO: Move this into a task runner class
foreach($config['source'] as $source) {
    if (!isset($source['type'])) {
        exit('source type node is required');
    }

    if (!isset($source['bucket'])) {
        exit('source bucket node is required');
    }

    if (!isset($source['region'])) {
        exit('source region node is required');
    }

    if (!isset($source['access'])) {
        exit('source access node is required');
    }

    if (!isset($source['secret'])) {
        exit('source secret node is required');
    }

    // create AWS client
    $client = new \Aws\S3\S3Client([
        'region'  => $source['region'],
        'version' => 'latest',
        'credentials' => [
            'key'    => $source['access'],
            'secret' => $source['secret'],
        ],
    ]);

    $aPairedS3Objects = s3Iterate($client, $source);

    processObfuscation($client, $aPairedS3Objects, $source, $dbConnection, $config['database']);
}

function s3Iterate(\Aws\S3\S3Client $sourceClient, array $source) {
    $params = array(
        'Bucket' => $source['bucket']
    );

    if (isset($source['directory'])) {
        $params['Prefix'] = $source['directory'];
    }

    $objects = $sourceClient->getIterator('ListObjects', $params);

    $pairedObjects = [];

    foreach ($objects as $object) {
        pairS3Objects($object, $pairedObjects);
    }

    return $pairedObjects;
}

function pairS3Objects(array $s3object, array &$pairedObjects) {
    $ext = pathinfo($s3object['Key'], PATHINFO_EXTENSION);

    $pairedObjects[dirname($s3object['Key'])][$ext] = basename($s3object['Key']);
} 

function processObfuscation(\Aws\S3\S3Client $sourceClient, array $pairedObjects, array $source, mysqli $dbConnection, array $dbConnectionDetails) {

    foreach($pairedObjects as $path => $pair) {
        if (!isset($pair['sql'])) {
            errorMessage("✗ Skipping $path as there is no sql file");
            continue;
        }

        if (!isset($pair['yml'])) {
            errorMessage("✗ Skipping $path as there is no yml file");
            continue;
        }

        progressMessage('========================');
        progressMessage('Processing ' . $path);

        try {
            $databaseName = preg_replace('/[^A-Za-z0-9\-]/', '', $path);
            /**
             * 
             *  Download and parse manifest file
             * 
             */
            progressMessage('• Downloading manifest file');
            $result = $sourceClient->getObject([
                'Bucket'     => $source['bucket'],
                'Key'        => $path . DIRECTORY_SEPARATOR . $pair['yml'],
                'SaveAs'     => MANIFEST_FILE,
            ]);

            progressMessage('✓ Downloaded manifest file');

            $manifest = Yaml::parseFile(MANIFEST_FILE);

            progressMessage('✓ Parsed manifest.yml');

            if (!isset($manifest['destination'])) {
                errorMessage('destination node is required', true);
            }

            if (!is_array($manifest['destination'])) {
                errorMessage('destination node must be an array', true);
            }

            if (!isset($manifest['destination']['type'])) {
                errorMessage('destination type node is required', true);
            }

            if (!isset($manifest['destination']['bucket'])) {
                errorMessage('destination bucket node is required', true);
            }

            if (!isset($manifest['destination']['region'])) {
                errorMessage('destination region node is required', true);
            }

            if (!isset($manifest['destination']['access'])) {
                errorMessage('destination access node is required', true);
            }

            if (!isset($manifest['destination']['secret'])) {
                errorMessage('destination secret node is required', true);
            }

            if (!isset($manifest['destination']['dir'])) {
                errorMessage('destination dir node is required', true);
            }

            if (!isset($manifest['destination']['structure_filename'])) {
                errorMessage('destination structure_filename node is required', true);
            }

            if (!isset($manifest['destination']['data_filename'])) {
                errorMessage('destination data_filename node is required', true);
            }

            if (DRY_RUN) {
                foreach($manifest['data'] as $database) {
                    foreach ($database as $tableName => $table) {
                        foreach($table as $obfuscationType => $fields) {
                            obfuscateField($dbConnection, $tableName, $obfuscationType, $fields, DRY_RUN);
                        }

                        progressMessage('✓ Completed obfuscation of ' . $tableName);
                    }
                }
                continue;
            }

            /**
             * 
             *  Download and process sql file
             * 
             */
            progressMessage('• Downloading sql files');
            $sourceClient->getObject([
                'Bucket'     => $source['bucket'],
                'Key'        => $path . DIRECTORY_SEPARATOR . SQL_STRUCTURE_FILE,
                'SaveAs'     => STORAGE_DIR . SQL_STRUCTURE_FILE
            ]);

            $sourceClient->getObject([
                'Bucket'     => $source['bucket'],
                'Key'        => $path . DIRECTORY_SEPARATOR . SQL_DATA_FILE,
                'SaveAs'     => STORAGE_DIR . SQL_DATA_FILE
            ]);

            progressMessage('✓ Downloaded sql files');

            cleanUpDatabase($dbConnection, $databaseName);

            if (!$dbConnection->multi_query('CREATE DATABASE ' . $databaseName)) {
                throw new \Exception(
                    'DB Error message: ' . $dbConnection->error . ' (creating database ' . $databaseName . ')'
                );
            }

            progressMessage('✓ Created database ' . $databaseName);

            if (!$dbConnection->select_db($databaseName)) {
                throw new \Exception('DB Error message: Can\'t select database ' . $databaseName);
            }

            progressMessage('✓ Selected database');

            $sql = 'SET FOREIGN_KEY_CHECKS=0;';

            if (!$dbConnection->multi_query($sql)) {
                throw new \Exception('DB Error message: ' . $dbConnection->error . ' (' . $sql . ')');
            }

            progressMessage('✓ Turned off foreign key checks');


            $sql = file_get_contents(STORAGE_DIR . SQL_STRUCTURE_FILE);
            if (!$dbConnection->multi_query($sql)) {
                throw new \Exception('DB Error message: ' . $dbConnection->error . ' (creating table structure)');
            }

            progressMessage('✓ Created DB structure'); 

            // split file into smaller parts so we can avoid importing a huge file
            if (!is_dir(STORAGE_PARTS_DIR)) {
                mkdir(STORAGE_PARTS_DIR);
            }
            exec("split --lines=500 " . STORAGE_DIR . SQL_DATA_FILE . " " . STORAGE_PARTS_DIR);

            $dbPartFiles = array_diff(scandir(STORAGE_PARTS_DIR), array('..', '.'));

            progressMessage('✓ DB dump has been split into ' . count($dbPartFiles) . ' parts');

            foreach ($dbPartFiles as $filename) {
                $sql = file_get_contents(STORAGE_PARTS_DIR . $filename);

                if (!$dbConnection->multi_query($sql)) {
                    throw new \Exception('DB Error message: ' . $dbConnection->error . ' when importing db part ' . $filename);
                }

                progressMessage('✓ Imported DB part ' . $filename);

                do {
                    if ($result = $dbConnection->store_result()) {
                        while ($row = $result->fetch_row()) {
                            printf("%s\n", $row[0]);    
                        }
                        $result->free();
                    }

                    $dbConnection->next_result();
                } while ($dbConnection->more_results());

                progressMessage('✓ Freed import results');
            }

            progressMessage('✓ Imported DB dump');


            foreach($manifest['data'] as $database) {
                foreach ($database as $tableName => $table) {
                    foreach($table as $obfuscationType => $fields) {
                        obfuscateField($dbConnection, $tableName, $obfuscationType, $fields);
                    }

                    progressMessage('✓ Completed obfuscation of ' . $tableName);
                }
            }





            // create AWS client for pushing to destination
            $destinationClient = new \Aws\S3\S3Client([
                'region'  => $manifest['destination']['region'],
                'version' => 'latest',
                'credentials' => [
                    'key'    => $manifest['destination']['access'],
                    'secret' => $manifest['destination']['secret'],
                ],
            ]);

            progressMessage('✓ Connected to destination');





            // dump the DB data file locally
            $dump = new IMysqldump\Mysqldump(
                'mysql:host=' . $dbConnectionDetails['host'] . ':' . $dbConnectionDetails['port'] . ';dbname=' . $databaseName,
                $dbConnectionDetails['user'],
                $dbConnectionDetails['password'],
                [
                    'add-drop-table' => true,
                    'no-create-info' => true
                ]
            );
            $dump->start(CLEANSED_DB_FILE);

            progressMessage('✓ Dumped cleansed database');

            // push the data file to storage
            $result = $destinationClient->putObject([
                'Bucket'     => $manifest['destination']['bucket'],
                'Key'        => $manifest['destination']['dir'] . DIRECTORY_SEPARATOR . $manifest['destination']['data_filename'],
                'SourceFile' => CLEANSED_DB_FILE,
            ]);

            progressMessage('✓ Pushed cleansed data DB to destination');

            // push the structure file to storage
            $result = $destinationClient->putObject([
                'Bucket'     => $manifest['destination']['bucket'],
                'Key'        => $manifest['destination']['dir'] . DIRECTORY_SEPARATOR . $manifest['destination']['structure_filename'],
                'SourceFile' => STORAGE_DIR . SQL_STRUCTURE_FILE,
            ]);

            progressMessage('✓ Pushed cleansed DB to destination');





















            // delete the manifest from S3, but not the DB dumps
            $result = $sourceClient->deleteObject([
                'Bucket'     => $source['bucket'],
                'Key'        => $path . DIRECTORY_SEPARATOR . $pair['yml']
            ]);

            progressMessage('✓ Deleted manifest from source');

        } catch(\Exception $e) {
            // raise error, but keep processing
            errorMessage($e->getMessage());
        }

        cleanUpFiles();
        cleanUpDatabase($dbConnection, $databaseName);

        progressMessage('========================');
    }
}

function progressMessage(string $message) {
    echo $message . PHP_EOL;
}

function errorMessage(string $message, $die = false) {
    echo $message . PHP_EOL;

    if ($die) {
        exit();
    }
}

function cleanUpFiles() {
    progressMessage('✓ Cleaning up files');

    foreach (glob(STORAGE_DIR . "*.sql") as $filename) {
        unlink($filename);
        progressMessage('✓ Removed DB file ' . $filename);
    }

    foreach (glob(STORAGE_PARTS_DIR . "*.sql") as $filename) {
        unlink($filename);
        progressMessage('✓ Removed DB part ' . $filename);
    }

    if (file_exists(MANIFEST_FILE)) {
        unlink(MANIFEST_FILE);
        progressMessage('✓ Removed manifest');
    }
}

function cleanUpDatabase(mysqli $dbConnection, $databaseName) {
    if (!$dbConnection->multi_query('DROP DATABASE IF EXISTS ' . $databaseName)) {
        errorMessage('DB Error message: ' . $dbConnection->error . ' when dropping db ' . $databaseName);
    }

    progressMessage('✓ Removed database ' . $databaseName);
}

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
        if (!$dbConnection->multi_query($query)) {
            throw new \Exception('DB Error message: ' . $dbConnection->error . PHP_EOL . 'Query with error: ' . $query);
        }
    }

    progressMessage("✓ Obfuscated $obfuscationType fields in $tableName");
}

echo PHP_EOL;
exit;
