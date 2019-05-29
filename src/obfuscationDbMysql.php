<?php

use Ifsnop\Mysqldump as IMysqldump;

class obfuscationDbMysql extends obfuscationDb
{
    /**
     * @var mysqli
     */
    private $dbConnection;

    /**
     * Constructor
     * @param string $host 
     * @param int $port 
     * @param string $user 
     * @param string $password 
     * @return void
     */
    public function __construct(
        string $host,
        int $port,
        string $user,
        string $password
    ) {
        $this->host = $host;
        $this->port = $port;
        $this->user = $user;
        $this->password = $password;

        $this->dbConnection = new mysqli(
            $host . ':' . $port,
            $user,
            $password
        );

        if ($this->dbConnection->connect_errno) {
            throw new \Exception('Database connection failed: ' . $this->dbConnection->connect_error, true);
        }

        $this->run('SET @@global.max_allowed_packet = 524288000');
    }

    /**
     * Run a query against the database
     * @param string $sql 
     * @return void
     */
    public function run(string $sql)
    {
        while ($this->dbConnection->more_results()
            && $this->dbConnection->next_result()
        ) {
            ; // flush multi_queries
        }

        if (!$this->dbConnection->multi_query($sql)) {
            $this->logger->errorMessage(
                'Could not run sql ' . $this->dbConnection->error . PHP_EOL . PHP_EOL . $sql, true
            );
        }
    }

    /**
     * Selects the database to work on
     * @param string $dbname 
     * @return void
     */
    public function selectDb(string $dbName)
    {
        if (!$this->dbConnection->select_db($dbName)) {
            throw new \Exception('DB Error message: Can\'t select database ' . $dbName);
        }
    }

    /**
     * Dumps data from a database
     * @param string $dbName 
     * @param string $outFile 
     * @return void
     */
    public function dumpDbData(string $dbName, string $outFile) 
    {
        // dump the DB data file locally
        $dump = new IMysqldump\Mysqldump(
            'mysql:host=' . $this->host . ':' . $this->port . ';dbname=' . $dbName,
            $this->user,
            $this->password,
            [
                'add-drop-table' => true,
                'no-create-info' => true
            ]
        );
        
        $dump->start($outFile);
    }
}
