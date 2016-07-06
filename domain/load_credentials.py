def load_aws_keys():
    with open("config/AWS_key", "r") as f:
        lines = f.readlines()
        aws_s3_path = lines[0].split('=')[1].strip()
        aws_access_key = lines[1].split('=')[1].strip()
        aws_secret_key = lines[2].split('=')[1].strip()
        return aws_s3_path, aws_access_key, aws_secret_key
    raise IOError("AWS identity could not be loaded.")


def load_key():
    with open("config/api_key", "r") as file:
        return file.readlines()[0].strip()