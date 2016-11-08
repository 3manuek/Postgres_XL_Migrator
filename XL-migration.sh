#!/bin/bash

#######################################################################################################
#
# Script: XL-migration.sh
#
# Description: The purpose of this script is to migrate data from regular Postgres
#	       to postgresXL environment where data will be distributed evenly across
#	       all nodes as much as possible. This script depends on the following files
#
#   1) XL-migration properties file: Migration configuration parameters
#   2) migration_analysis_fn.sql: Instead of relying on Postgres-XL defaults, this
#			     	  function will find the best candidate distribution
#				  column based on the table row counts.
#   3) genTblddl_fn.sql: Using information from migration_analysis function to produce
#			 table DDL script which will be used on the destination DB.
#
# Parms: migration properties file
#
# Note: This script assumes the destination database already existed.
#
#######################################################################################################
#set -x

################################################
#### log():  Write info to log file          ###
################################################
log()
{
   printf '%s\n\n' "$*";
}

################################################
#### error(): Write error to log file        ###
################################################
error()
{
  log "ERROR: $*";
  exit 3
}

################################################
#### Usage():  Show command line syntax      ###
################################################
Usage()
{
   log "Usage: ${PROG} [-h] [-c properties file]"
   log "-h               print this help and exit"
   log "-c               full path to migration properties file (default to ./XL-migration.properties)"
   exit 1;
}

##################################################################
### cr_fn_migration(): Create function to collect distribution ###
### column information					       ###
##################################################################
cr_fn_migration()
{
	TEMP_SCHEMA=$1

	# Create table if not exists. This table is used to store temporary migration data within the function
	EXISTS_FLAG=$(psql -U ${SRC_USER} -h ${SRC_HOST} ${SRC_DB} -At -q -c \
					"SELECT EXISTS(SELECT table_name \
					 FROM information_schema.tables where table_schema = '${TEMP_SCHEMA}' \
					 AND table_name = '${TEMP_TABLE}');")
	if [ "${EXISTS_FLAG}" == "f" ]
	then
		# Does not exist, create table
		log "===== ${TEMP_SCHEMA}.${TEMP_TABLE} table does not exist, creating it........`date`"
		psql -U ${SRC_USER} -h ${SRC_HOST} ${SRC_DB} -At -q -c \
					"CREATE TABLE ${TEMP_SCHEMA}.${TEMP_TABLE}(str_schema varchar(128) \
                               						    ,str_table varchar(128) \
                               						    ,str_column varchar(128) \
                               						    ,int_total_rows int \
                               						    ,flt_perc_diff FLOAT \
                               						    ,str_candidate varchar(3));"
	else
		# Table exists, truncate the data
		log "===== ${TEMP_SCHEMA}.${TEMP_TABLE} exists, cleaning up old data........`date`"
		psql -U ${SRC_USER} -h ${SRC_HOST} ${SRC_DB} -At -q -c "TRUNCATE ${TEMP_SCHEMA}.${TEMP_TABLE};"
	fi

	# Install migration functions but first drop them if they exist
	log "===== Installing utility functions........`date`"
	psql -U ${SRC_USER} -h ${SRC_HOST} ${SRC_DB} -At -q -c "DROP FUNCTION IF EXISTS ${TEMP_SCHEMA}.migration_analysis(varchar);"	
	psql -U ${SRC_USER} -h ${SRC_HOST} ${SRC_DB} -At -q -c "DROP FUNCTION IF EXISTS ${TEMP_SCHEMA}.genTblddl(varchar,varchar,varchar,varchar);"	

	# Replacing %SCHEMA% values in both migration_analysis_fn.sql and genTblddl.sql files
	sed -e "s/%SCHEMA%/${TEMP_SCHEMA}/g" -e "s/%TABLE%/${TEMP_TABLE}/g" migration_analysis_fn.sql > ${DUMP_DIR}/migration_analysis_fn.sql
	sed -e "s/%SCHEMA%/${TEMP_SCHEMA}/g" genTblddl_fn.sql > ${DUMP_DIR}/genTblddl_fn.sql
	# Create the functions
	psql -U ${SRC_USER} -h ${SRC_HOST} ${SRC_DB} -f ${DUMP_DIR}/migration_analysis_fn.sql
	psql -U ${SRC_USER} -h ${SRC_HOST} ${SRC_DB} -f ${DUMP_DIR}/genTblddl_fn.sql

	# Generate column distribution information
	log "===== Generating column distribution data........`date`"
	psql -U ${SRC_USER} -h ${SRC_HOST} ${SRC_DB} -At -q -c "SELECT ${TEMP_SCHEMA}.migration_analysis('${MIGRATE_SCHEMA}');"

}

###############################################################
### migrate_data(): Function to migrate data                 ##
###############################################################
migrate_data()
{
	TEMP_SCHEMA=$1

	# Create schema on destination DB if not already exists
	QUERY="select exists (select distinct(schema_name) from information_schema.schemata \
				where schema_name = '${MIGRATE_SCHEMA}');"

	EXIST_FLAG=$(psql -U ${DEST_USER} -h ${DEST_HOST} ${DEST_DB} -At -q -c "${QUERY}")

	if [ "${EXIST_FLAG}" == "f" ]
	then
		log "===== Schema ${MIGRATE_SCHEMA} does not exist, creating it........`date`"
		psql -U ${DEST_USER} -h ${DEST_HOST} ${DEST_DB} -At -q -c "CREATE SCHEMA ${MIGRATE_SCHEMA} authorization ${SCHEMA_OWNER};"
		psql -U ${DEST_USER} -h ${DEST_HOST} ${DEST_DB} -At -q -c "GRANT ALL ON SCHEMA ${MIGRATE_SCHEMA} TO ${SCHEMA_OWNER};"
		psql -U ${DEST_USER} -h ${DEST_HOST} ${DEST_DB} -At -q -c "GRANT USAGE ON SCHEMA ${MIGRATE_SCHEMA} TO ${SCHEMA_OWNER};"
	fi

	for tblname in ${TABLES}
	do
		#Get distribution column name for the table
		log "===== Retrieving distribution column for table ${tblname}........`date`"
		DIST_COLUMN=$(psql -U ${SRC_USER} -h ${SRC_HOST} ${SRC_DB} -At -q -c "SELECT COALESCE(str_column,'x') FROM ${TEMP_SCHEMA}.${TEMP_TABLE} \
								    WHERE str_schema = '${MIGRATE_SCHEMA}' \
								    AND str_table = '${tblname}' \
								    AND str_candidate = 'Yes';")
		# Generate CREATE DDL 
		log "===== Generate CREATE DDL script........`date`"
		if [ "${DIST_TYPE}x" == "x" ]
		then
			# Distribution type is not specified, default to hash
			DIST_TYPE="hash"
		fi

		psql -U ${SRC_USER} -h ${SRC_HOST} ${SRC_DB} -At -q -c \
				"select ${TEMP_SCHEMA}.genTblddl('${MIGRATE_SCHEMA}','${tblname}','${DIST_COLUMN}','${DIST_TYPE}');" -o ${DUMP_DIR}/${tblname}.sql

		# Create table on XL destination environment
		log "===== Creating table ${tblname} in ${DEST_DB} database........`date`"
		psql -U ${DEST_USER} -h ${DEST_HOST} ${DEST_DB} -At -q -c "DROP TABLE IF EXISTS ${MIGRATE_SCHEMA}.${tblname} cascade;"
		psql -U ${DEST_USER} -h ${DEST_HOST} ${DEST_DB} -At -q -f ${DUMP_DIR}/${tblname}.sql

		# Grant permissions (assuming read only and update roles already exist)
		log "===== Granting permissions..............`date`"
		psql -U ${DEST_USER} -h ${DEST_HOST} ${DEST_DB} -At -q -c "ALTER TABLE ${MIGRATE_SCHEMA}.${tblname} OWNER TO ${SCHEMA_OWNER};"
		psql -U ${DEST_USER} -h ${DEST_HOST} ${DEST_DB} -At -q -c "GRANT ALL ON TABLE ${MIGRATE_SCHEMA}.${tblname} TO ${SCHEMA_OWNER};"
		psql -U ${DEST_USER} -h ${DEST_HOST} ${DEST_DB} -At -q -c "GRANT SELECT ON TABLE ${MIGRATE_SCHEMA}.${tblname} TO ${SCHEMA_OWNER}_ro;"
		psql -U ${DEST_USER} -h ${DEST_HOST} ${DEST_DB} -At -q -c "GRANT SELECT,INSERT,UPDATE,DELETE ON TABLE \
										${MIGRATE_SCHEMA}.${tblname} TO ${SCHEMA_OWNER}_update;"

		# Extract data from source
		log "===== Exporting data from ${SRC_DB}........`date`"
		pg_dump -a -Fc -O -U ${SRC_USER} -h ${SRC_HOST} -t ${MIGRATE_SCHEMA}.${tblname} -f ${DUMP_DIR}/${tblname}.dmp ${SRC_DB}

		# Extract data from source
		log "===== Restoring data to ${DEST_DB}........`date`"
		pg_restore -j4 -U ${DEST_USER} -h ${DEST_HOST} -d ${DEST_DB} ${DUMP_DIR}/${tblname}.dmp
	done
}


#####################################################################
### get_work_area: Function to find a temporary working schema    ###
#####################################################################
get_work_area()
{
	QUERY="select exists (select distinct(schema_name) from information_schema.schemata \
				where schema_name = 'temp');"

	EXISTS=$(psql -U ${SRC_USER} -h ${SRC_HOST} ${SRC_DB} -At -q -c "${QUERY}")

	echo ${EXISTS}
}

########################################################################
### cleanUp(): Function to clean up temporary DB objects and files   ###
########################################################################
cleanUp()
{
	TEMP_SCHEMA=$1
	rm -rf ${DUMP_DIR}
	psql -U ${SRC_USER} -h ${SRC_HOST} ${SRC_DB} -At -q -c "DROP TABLE IF EXISTS ${TEMP_SCHEMA}.${TEMP_TABLE} cascade;"
	psql -U ${SRC_USER} -h ${SRC_HOST} ${SRC_DB} -At -q -c "DROP FUNCTION IF EXISTS ${TEMP_SCHEMA}.migration_analysis(varchar);"	
	psql -U ${SRC_USER} -h ${SRC_HOST} ${SRC_DB} -At -q -c "DROP FUNCTION IF EXISTS ${TEMP_SCHEMA}.genTblddl(varchar,varchar,varchar,varchar);"	
}

####################
### MAIN PROGRAM ###
####################

PROG=`basename $0`
PROP_FILE=XL-migrate.properties

ARGS=`getopt hc: $*`

if [ $? != 0 ] ; then
  Usage
  exit 1
fi

set -- $ARGS

while [ "$1" != -- ] ; do
  case "$1" in
    -h)   Usage ; exit 0 ;;
    -c)   export PROP_FILE="$2";shift ;;
    *)    Usage ; exit 1 ;;
  esac
  shift
done
shift

if [ -r ${PROP_FILE} ]
then
        source ${PROP_FILE}
else
        error "Unreadable properties file: ${PROP_FILE}"
        Usage
        exit 1;
fi

log "===== Start Migration process........`date`"

# Create a working directory if not exist
mkdir -p ${DUMP_DIR}

# Check to see if TEMP schema exist, otherwise use PUBLIC instead
# This will be used as a temporary work space in the database
EXIST=$(get_work_area)
if [ "${EXIST}" = "f" ]
then
	WORK_AREA="public"
else
	WORK_AREA="temp"
fi

# Create function to collect distribution column information
cr_fn_migration ${WORK_AREA}

# Migrate data
migrate_data ${WORK_AREA}

# Clean up temporary database objects and files
if [ "${CLEAN_FLAG}" = "yes" ]
then
	cleanUp ${WORK_AREA}
fi

log "===== Migration process is complete........`date`"

exit 0
