import pysftp
from cep_credentials import sftp_dev_password, sftp_dev_user, sftp_pro_password, sftp_pro_user, sftp_host

with pysftp.Connection(host=sftp_host, username=sftp_dev_user, password=sftp_dev_password, ) as sftp:
    print("Connection successfully established ... ")
    # Switch to a remote directory
    sftp.cwd('pro')
    sftp.put("20210907110501_cepsa_update.txt")
    sftp.cwd('/')
    files = sftp.listdir()
    for dir in files:
        sftp.cwd(dir)
        print(sftp.listdir())
        sftp.cwd('..')

    print(files)


with pysftp.Connection(host=sftp_host, username=sftp_pro_user, password=sftp_pro_password, ) as sftp:
    print("Connection successfully established ... ")
    # Switch to a remote directory
    files = sftp.listdir()
    for dir in files:
        sftp.cwd(dir)
        print(sftp.listdir())
        sftp.cwd('..')

    print(files)

with pysftp.Connection(host=sftp_host, username=sftp_dev_user, password=sftp_dev_password, ) as sftp:
    print("Connection successfully established ... ")
    # Switch to a remote directory
    sftp.cwd('pro')
    sftp.rename("20210907110501_cepsa_update.txt", "20210907110501_cepsa_update_renamed.txt")
    sftp.remove("20210907110501_cepsa_update_renamed.txt")
    sftp.cwd('/')
    files = sftp.listdir()
    for dir in files:
        sftp.cwd(dir)
        print(sftp.listdir())
        sftp.cwd('..')
        