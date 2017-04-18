# stail - Stream Tail

A simple utility designed to mimic `tail -f` for Kinesis streams.

## installation

``` shell
mvn clean package
bash ./target/classes/build.sh
sudo cp ./target/stail /usr/local/bin
```

## usage

### options

```
Usage: stail [options]
  Options:
    --duration
      how long the stream should be tailed. eg: PT15M is 15mins
    --profile
      AWS profile to use for credentials
    --region
      AWS region to find the stream in
      Default: us-west-2
    --role
      role ARN to be assumed to connect to Kinesis
    --start
      time to start fetching records from relative to now. eg: PT15M is 15mins
      ago
  * --stream
      Kinesis stream name to tail
```

### example: getting airplanes from an object stream specifying which profile to use

```shell
stail --stream my-favourite-stream --region us-west-2 --profile dev | jq 'select(.type == "airplane")'
```

### example: using --role to assume the necessary role to read from Kinesis

```shell
stail --stream my-favourite-stream --role arn:aws:iam::12345678912345:role/my-favourite-stream-readonly-role
```

## known limitations

- currently doesnt handle resharding
