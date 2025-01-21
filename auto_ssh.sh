#!/usr/bin/expect

# Define variables for SSH
set bastion_user "zkdeng"
set bastion_host "hpc.arizona.edu"
set remote_user "zkdeng"
set remote_host "cpu23.elgato.hpc.arizona.edu"

# SSH into the bastion host
spawn ssh $bastion_user@$bastion_host

# Expect the bastion host prompt
expect -re {[$#] ?$} {
    # Send the "shell" command to the bastion host
    send "shell\r"
    expect -re {[$#] ?$} {
        # SSH into the specific machine
        send "ssh $remote_user@$remote_host\r"
    }
}

# Interact with the session
interact
