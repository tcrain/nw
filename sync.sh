ip=$1

rsync -avuz --exclude='log_files' --exclude='.git' --exclude='target' -e "ssh -i ~/.ssh/digitalocean" ./ $ip:~/Rust/nw/
rsync -avuz --exclude='log_files' --exclude='.git' --exclude='target' -e "ssh -i ~/.ssh/digitalocean" $ip:~/Rust/nw/ ./
