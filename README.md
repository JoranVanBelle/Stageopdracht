# Stageopdracht

Deze repository maakt deel uit van mijn stageopdracht en is de adapter die Meetnet Vlaamse Banken raadpleegt. Tijdens mijn stageopdracht moest er een event-driven architectuur (EDA) gebouwd worden. Andere repo's van mijn opdracht zijn [de API](https://github.com/JoranVanBelle/Stageopdracht-api) en [de UI](https://github.com/JoranVanBelle/Stageopdracht-UI).

# Inhoud

Deze code bevat een ``Dockerfile`` die het java project omvormt naar een docker image en een ``docker-compose.yml`` om de nodige containers op te zetten bij het lokaal runnen van deze repository.

# Starten

Om het project in docker te runnen moeten volgende stappen uitgevoerd worden in de rootfolder van het project:
  1. Project builden
    1.1 Windows: ``.\mvnw clean install``
    1.2 Linux: ``\mvnw clean install``
  
  2. Docker image maken: ``docker build -t adapter:latest .``

  3. Docker containers runnen: ``docker-compose up``

  4. Docker image runnen: ``docker run adapter:latest``

# Env variables

  Dit project vereist ook enkele variabele (om het in google cloud te runnen):

  - DATABASE_NAME
  - DATABASE_INSTANCE_CONNECTION_NAME
  - DATABASE_USERNAME
  - DATABASE_PASSWORD
  - USERNAME
  - PASSWORD
  - APP_ID
  - BOOTSTRAP_SERVERS
  - SCHEMA_REGISTRY_URL
  - EMAILHOST
  - EMAILPORT
  - EMAIL_USERNAME
  - EMAIL_PASSWORD
  - BASEURL
  - CLUSTER_API_KEY
  - CLUSTER_API_SECRET
  - SR_API_KEY
  - SR_API_SECRET
  - CREDENTIAL_SOURCE
  - RESET_CONFIG
  - TIMEOUT_MS
  - DNS_LOOKUP
  - SASL_MECHANISM
  - SECURITY_PROTOCOL
  - API