#!/usr/bin/env bash
if [ -z ${DATABASE_NAME} ]; then echo "DATABASE_NAME env var unset"; else echo "Database name is set to '$DATABASE_NAME'"; fi
if [ -z ${DATABASE_INSTANCE_CONNECTION_NAME} ]; then echo "DATABASE_INSTANCE_CONNECTION_NAME env var unset"; else echo "Database instance connection name is set to '$DATABASE_INSTANCE_CONNECTION_NAME'"; fi
if [ -z ${DATABASE_USERNAME} ]; then echo "DATABASE_USERNAME env var unset"; else echo "Database username is set to '$DATABASE_USERNAME'"; fi
if [ -z ${DATABASE_PASSWORD} ]; then echo "DATABASE_PASSWORD env var unset"; else echo "database password is set"; fi
if [ -z ${USERNAME} ]; then echo "USERNAME env var unset"; else echo "Username is set to '$USERNAME'"; fi
if [ -z ${PASSWORD} ]; then echo "PASSWORD env var unset"; else echo "Password is set"; fi
if [ -z ${APP_ID} ]; then echo "APP_ID env var unset"; else echo "App id set to '$APP_ID'"; fi
if [ -z ${BOOTSTRAP_SERVERS} ]; then echo "BOOTSTRAP_SERVERS env var unset"; else echo "Bootstrapservers set to '$BOOTSTRAP_SERVERS'"; fi
if [ -z ${SCHEMA_REGISTRY_URL} ]; then echo "SCHEMA_REGISTRY_URL env var unset"; else echo "Schemaregistry url is set to '$SCHEMA_REGISTRY_URL'"; fi
if [ -z ${EMAILHOST} ]; then echo "EMAILHOST env var unset"; else echo "Emailhost is set to '$EMAILHOST'"; fi
if [ -z ${EMAILPORT} ]; then echo "EMAILPORT env var unset"; else echo "Emailport is set to '$EMAILPORT'"; fi
if [ -z ${EMAIL_USERNAME} ]; then echo "EMAIL_USERNAME env var unset"; else echo "Email username set to '$EMAIL_USERNAME'"; fi
if [ -z ${EMAIL_PASSWORD} ]; then echo "EMAIL_PASSWORD env var unset"; else echo "Emailpassword is set"; fi
if [ -z ${BASEURL} ]; then echo "BASEURL env var unset"; else echo "Baseurl is set to set to '$BASEURL'"; fi
if [ -z ${CLUSTER_API_KEY} ]; then echo "CLUSTER_API_KEY env var unset"; else echo "Cluster api key is set"; fi
if [ -z ${CLUSTER_API_SECRET} ]; then echo "CLUSTER_API_SECRET env var unset"; else echo "Clusert api secret is set"; fi
if [ -z ${SR_API_KEY} ]; then echo "SR_API_KEY env var unset"; else echo "Sr api key is set"; fi
if [ -z ${SR_API_SECRET} ]; then echo "SR_API_SECRET env var unset"; else echo "Sr api secret is set"; fi
if [ -z ${CREDENTIAL_SOURCE} ]; then echo "CREDENTIAL_SOURCE env var unset"; else echo "credential source is set to '$CREDENTIAL_SOURCE'"; fi
if [ -z ${RESET_CONFIG} ]; then echo "RESET_CONFIG env var unset"; else echo "Resetconfig set to '$RESET_CONFIG'"; fi
if [ -z ${TIMEOUT_MS} ]; then echo "TIMEOUT_MS env var unset"; else echo "Timeout ms is set to '$TIMEOUT_MS'"; fi
if [ -z ${DNS_LOOKUP} ]; then echo "DNS_LOOKUP env var unset"; else echo "Dns lookup is set to '$DNS_LOOKUP'"; fi
if [ -z ${SASL_MECHANISM} ]; then echo "SASL_MECHANISM env var unset"; else echo "Sasl mechanism set to '$SASL_MECHANISM'"; fi
if [ -z ${SECURITY_PROTOCOL} ]; then echo "SECURITY_PROTOCOL env var unset"; else echo "security protocol is set to '$SECURITY_PROTOCOL'"; fi

if [[ -z $DATABASE_NAME || -z $DATABASE_INSTANCE_CONNECTION_NAME || -z $DATABASE_USERNAME || -z $DATABASE_PASSWORD || -z $USERNAME || -z $PASSWORD || -z $APP_ID || -z $BOOTSTRAP_SERVERS || -z $SCHEMA_REGISTRY_URL || -z $EMAILHOST || -z $EMAILPORT || -z $EMAIL_USERNAME || -z $EMAIL_PASSWORD || -z $BASEURL || -z $CLUSTER_API_KEY || -z $CLUSTER_API_SECRET || -z $SR_API_KEY || -z $SR_API_SECRET || -z $CREDENTIAL_SOURCE || -z $RESET_CONFIG || -z $TIMEOUT_MS || -z $DNS_LOOKUP || -z $SASL_MECHANISM || -z $SECURITY_PROTOCOL ]]; then
    echo "one or more variables are undefined - Exiting"
    exit 1
else
    java -jar /App.jar $DATABASE_NAME $DATABASE_INSTANCE_CONNECTION_NAME $DATABASE_USERNAME $DATABASE_PASSWORD $USERNAME $PASSWORD $APP_ID $BOOTSTRAP_SERVERS $SCHEMA_REGISTRY_URL $EMAILHOST $EMAILPORT $EMAIL_USERNAME $EMAIL_PASSWORD $BASEURL $CLUSTER_API_KEY $CLUSTER_API_SECRET $SR_API_KEY $SR_API_SECRET $CREDENTIAL_SOURCE $RESET_CONFIG $TIMEOUT_MS $DNS_LOOKUP $SASL_MECHANISM $SECURITY_PROTOCOL
fi