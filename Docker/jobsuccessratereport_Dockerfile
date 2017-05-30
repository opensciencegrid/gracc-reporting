# FIFE base image for gracc-reporting
FROM shreyb/gracc-reporting:fife_0.9-2

# Run executable - Note that we expect the config file to be at /home/ifmon/fife-config/fife.config
ENTRYPOINT ["sh", "-c", "jobsuccessratereport -E $VO -s \"$START\" -e \"$END\" -c /home/ifmon/fife-config/fife.config"]
