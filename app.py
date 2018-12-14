from flask import Flask
from flask import render_template
from flask import request
# import mongo_stuff

app = Flask(__name__)

@app.route('/')
def hello_world():
    return render_template("main.html")

@app.route('/search')
def search():
    # search_query = request.args.get("query")
    # messages = mongo_stuff.search_get_messages(search_query)
    # sorted_messages = mongo_stuff.sorted_messages(messages, search_query)
    return render_template("search.html", message_list = sorted_messages)