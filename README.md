# llm-experiments


Repo to house generative AI experiments using OpenAI's GPT model, GCP's PaLM model, Langchain and more. 


# Getting started

### pyenv
Install a virtual env manager, ie:

```
brew update
brew install pyenv

# for zsh
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.zshrc
echo 'command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.zshrc
echo 'eval "$(pyenv init -)"' >> ~/.zshrc

# for bash
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
echo 'command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
echo 'eval "$(pyenv init -)"' >> ~/.bashrc

exec "$SHELL"

pyenv install 3.10.4

pyenv local 3.10.4
```


### poetry

This project is managed with [poetry](https://python-poetry.org/). It's a package manager and does a very good job of isolating
development environments for development and production.

To setup, install poetry:

```
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python3.8.3
PATH=$PATH:~/.poetry/bin/poetry
cat > .bash_profile
echo 'export PATH="$HOME/.poetry/bin:$PATH"' >> ~/.bash_profile
```
followed by:
```
make .venv
```


# Features

* TODO

# Credits

This package was created with Cookiecutter and the `my org/ML_CookiecutterPyPackage` project template.

* Cookiecutter: https://github.com/cookiecutter/cookiecutter
* my org/ML_CookiecutterPyPackage: https://github.com/my_org/ML_CookiecutterPyPackage
