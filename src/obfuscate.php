<?php

include_once('./vendor/autoload.php');

use Symfony\Component\Yaml\Yaml;
use Ifsnop\Mysqldump as IMysqldump;

define('DB_FILE', sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'sql.sql');
define('CLEANSED_DB_FILE', sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'clean.sql');
define('MANIFEST_FILE', sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'manifest.yml');
define('TMP_DB_NAME', 'derek');

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

$dbConnection = new mysqli(
        $config['database']['host'] . ':' . $config['database']['port'],
        $config['database']['user'],
        $config['database']['password']
    );

if ($dbConnection->connect_errno) {
    errorMessage('Database connection failed: ' . $dbConnection->connect_error, true);
}


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
    $objects = $sourceClient->getIterator('ListObjects', array(
        'Bucket' => $source['bucket']
    ));

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
            /**
             * 
             *  Download and parse manifest file
             * 
             */
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

            if (!isset($manifest['destination']['filename'])) {
                errorMessage('destination filename node is required', true);
            }

            /**
             * 
             *  Download and process sql file
             * 
             */
            $result = $sourceClient->getObject([
                'Bucket'     => $source['bucket'],
                'Key'        => $path . DIRECTORY_SEPARATOR . $pair['sql'],
                'SaveAs'     => DB_FILE,
            ]);

            progressMessage('✓ Downloaded sql file');

            if (!$dbConnection->query('CREATE DATABASE ' . TMP_DB_NAME)) {
                throw new \Exception('DB Error message: ' . $dbConnection->error);
            }

            progressMessage('✓ Created database ' . TMP_DB_NAME);

            if (!$dbConnection->select_db(TMP_DB_NAME)) {
                throw new \Exception('DB Error message: Can\'t select database ' . TMP_DB_NAME);
            }

            progressMessage('✓ Selected database');

            $sql = file_get_contents(DB_FILE);

            if (!$dbConnection->multi_query($sql)) {
                throw new \Exception('DB Error message: ' . $dbConnection->error);
            }

            progressMessage('✓ Imported DB dump');

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


            foreach($manifest['data'] as $database) {
                foreach ($database as $tableName => $table) {
                    foreach($table as $obfuscationType => $fields) {
                        obfuscateField($dbConnection, $tableName, $obfuscationType, $fields);
                    }

                    progressMessage('✓ Completed obfuscation of ' . $tableName);
                }
            }

            // dump the DB file locally
            $dump = new IMysqldump\Mysqldump(
                'mysql:host=' . $dbConnectionDetails['host'] . ':' . $dbConnectionDetails['port'] . ';dbname=' . TMP_DB_NAME,
                $dbConnectionDetails['user'],
                $dbConnectionDetails['password']
            );
            $dump->start(CLEANSED_DB_FILE);

            progressMessage('✓ Dumped cleansed database');

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

            // push the file to storage
            $result = $destinationClient->putObject([
                'Bucket'     => $manifest['destination']['bucket'],
                'Key'        => $manifest['destination']['dir'] . DIRECTORY_SEPARATOR . $manifest['destination']['filename'],
                'SourceFile' => CLEANSED_DB_FILE,
            ]);

            progressMessage('✓ Pushed cleansed DB to destination');

            // delete the manifest from S3, but not the DB dump
            $result = $sourceClient->deleteObject([
                'Bucket'     => $source['bucket'],
                'Key'        => $path . DIRECTORY_SEPARATOR . $pair['yml']
            ]);

            progressMessage('✓ Deleted manifest from source');

        } catch(\Exception $e) {
            // raise error, but keep processing
            errorMessage($e->getMessage());
        }
    }

    cleanUpFiles();
    cleanUpDatabase($dbConnection);

    progressMessage('========================');
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
    if (file_exists(DB_FILE)) {
        unlink(DB_FILE);
    }
    if (file_exists(MANIFEST_FILE)) {
        unlink(MANIFEST_FILE);
    }
}

function cleanUpDatabase(mysqli $dbConnection) {
    progressMessage('✓ Cleaning up database');
    if (!$dbConnection->query('DROP DATABASE IF EXISTS ' . TMP_DB_NAME)) {
        errorMessage('DB Error message: ' . $dbConnection->error);
    }
}

function obfuscateField(mysqli $dbConnection, string $tableName, string $obfuscationType, array $fields) {
    $fieldUpdates = [];

    foreach($fields as $field) {
        switch($obfuscationType) {
            case 'email':
                $fieldUpdates[] = "$field = concat(LEFT(UUID(), 8), '@example.com')";
                break;
            case 'string':
            default:
                $fieldUpdates[] = "$field = LEFT(UUID(), 8)";
                break;
        }
    }

    if (!$dbConnection->query('UPDATE ' . $tableName . ' set ' . implode(',', $fieldUpdates) . ' WHERE 1=1')) {
        throw new \Exception('DB Error message: ' . $dbConnection->error);
    }

    progressMessage("✓ Obfuscated $obfuscationType fields in $tableName");
}

echo PHP_EOL;
