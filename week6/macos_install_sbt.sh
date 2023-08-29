wget https://github.com/sbt/sbt/releases/download/v1.6.2/sbt-1.6.2.zip
unzip sbt-1.6.2.zip
sudo mv sbt /usr/local/
rm sbt-1.6.2.zip
echo 'SBT_HOME=/usr/local/sbt'>>~/.zshrc
echo 'PATH=$PATH:$SBT_HOME/bin'>>~/.zshrc
source ~/.zshrc
sbt sbtVersion