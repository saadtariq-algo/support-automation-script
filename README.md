# support-automation-script
This is the automation script repo
To use the automation script clone the repository and do the following steps
1. Go to root directory of this repo and create a virtual environment using this command -->> python3 -m venv your_environment_name (It is assumed you have already    installed venv in your machine otherwise this command will not work you have to install venv first).
2. After creating your virtual environment, activate the environment -->> source your_environment_name/bin/activate
3. Install the requirments using this command -->> pip install -r requirements.txt (if you get error i.e (error: invalid command 'bdist_wheel') do "pip install        wheel" and again run command -->> pip install -r requirements.txt
4. Update cofig.py file with your mongoDB credentials at 'uri' line. 
5. Now you can run the automation script using this command -->> python auto.py 
# support-automation-script
