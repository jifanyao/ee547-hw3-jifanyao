if [ $# -ne 2 ]; then
    echo "Usage: $0 <key_file> <ec2_public_ip>"
    exit 1
fi

KEY_FILE="$1"
EC2_IP="$2"

echo "Deploying to EC2 instance: $EC2_IP"

# Copy files
scp -i "$KEY_FILE" api_server.py ec2-user@"$EC2_IP":~
scp -i "$KEY_FILE" requirements.txt ec2-user@"$EC2_IP":~

# Install dependencies and start server
ssh -i "$KEY_FILE" ec2-user@"$EC2_IP" << 'EOF'
  # Install Python if necessary
  if command -v yum >/dev/null 2>&1; then
      sudo yum install -y python3 python3-pip
  elif command -v apt-get >/dev/null 2>&1; then
      sudo apt-get update
      sudo apt-get install -y python3 python3-pip
  fi

  pip3 install --user -r requirements.txt

  # Kill existing server if running
  pkill -f api_server.py || true

  # Start server in background
  nohup python3 api_server.py 8080 > server.log 2>&1 &

  echo "Server started. Test locally with: curl http://localhost:8080/papers/recent?category=cs.LG&limit=5"
EOF

echo "Deployment complete."
echo "Test from your machine with:"
echo "  curl \"http://$EC2_IP:8080/papers/recent?category=cs.LG&limit=5\""
