# Notice: Repository Deprecation
This repository is deprecated and no longer actively maintained. It contains outdated code examples or practices that do not align with current MongoDB best practices. While the repository remains accessible for reference purposes, we strongly discourage its use in production environments.
Users should be aware that this repository will not receive any further updates, bug fixes, or security patches. This code may expose you to security vulnerabilities, compatibility issues with current MongoDB versions, and potential performance problems. Any implementation based on this repository is at the user's own risk.
For up-to-date resources, please refer to the [MongoDB Developer Center](https://mongodb.com/developer).


# Aggregation Pipeline: Applying Benford's Law to COVID-19 Data

Read more about this repository in this [blog post](https://developer.mongodb.com/article/aggregation-pipeline-covid19-benford-law/).

# Use these pipelines

- Connect to this cluster using `mongosh` or `Compass`.

```
mongodb+srv://readonly:readonly@covid-19.hip2i.mongodb.net/covid19
```

- Import the pipeline in Compass in the `covid19.countries_summary` collection or execute the script against the same collection in mongosh.

