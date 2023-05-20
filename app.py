from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage, FollowEvent, UnfollowEvent, ImageSendMessage
from google.cloud.sql.connector import Connector, IPTypes
from google.cloud import storage
import pytds
import sqlalchemy
import openai
import time, os
import base64
os.environ['TZ'] = 'Asia/Taipei'
time.tzset()

app = Flask(__name__)

openai.api_key = 'openai.api_key'
AccessToken = 'AccessToken'
ChannelSecret = 'ChannelSecret'

assistantTemplate = {"role": "assistant", "content": ""}

LineBotApi = LineBotApi(AccessToken)
Handler = WebhookHandler(ChannelSecret)

@app.route("/callback", methods=['POST'])
def callback(request):
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)

    app.logger.info("Request body: " + body)
    try:
        Handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'

@Handler.add(FollowEvent)
def handle_follow(event):
    user_id = event.source.user_id
    user_name = LineBotApi.get_profile(user_id).display_name

    # now = time.strftime("%Y-%m-%d %H:%M:%S")
    # sql_params = {"UserID": user_id, "UserName": user_name, "FollowDate": now}
    # engine = connect_with_connector()
    # try:
    #     insert_sql = sqlalchemy.text("INSERT INTO LineUserBasic (UserID, UserName, FollowDate) VALUES (:UserID, :UserName, :FollowDate)")
    #     with engine.connect() as conn:
    #         result = conn.execute(insert_sql, sql_params)
    #         conn.commit()
    # except:
    #     update_sql = sqlalchemy.text("UPDATE LineUserBasic SET UserName = :UserName, FollowDate = :FollowDate WHERE UserID = :UserID")
    #     with engine.connect() as conn:
    #         result = conn.execute(update_sql, sql_params)
    #         conn.commit()
    # engine.dispose()

    LineBotApi.reply_message(
        event.reply_token,
        TextSendMessage(text="歡迎使用本機器人，本機器人使用GPT-3.5-turbo模型，歡迎測試")
    )
    return "Ok"

@Handler.add(UnfollowEvent)
def handle_unfollow(event):
    # user_id = event.source.user_id
    # now = time.strftime("%Y-%m-%d %H:%M:%S")
    # sql_params = {"UserID": user_id, "UnFollowDate": now}

    # engine = connect_with_connector()
    # update_sql = sqlalchemy.text("UPDATE LineUserBasic SET UnFollowDate = :UnFollowDate WHERE UserID = :UserID")
    # with engine.connect() as conn:
    #     result = conn.execute(update_sql, sql_params)
    #     conn.commit()
    # engine.dispose()

    return "Ok"

@Handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_id = event.source.user_id
    if hasattr(event.source, 'group_id'):
        group_id = event.source.group_id
    else:
        group_id = 'self'
    user_message = ""
    if (event.message.text.startswith('#GPT')):
        user_message = event.message.text[4:]
        engine = connect_with_connector()
        messages = []
        with engine.connect() as conn:
            sqlText = """
SELECT
    CASE [AiFlag]
        WHEN 0 THEN '$UR:' + [Message]
        WHEN 1 THEN '$AI:' + [Message]
    END AS Result
FROM [LineGroupHistoryMessage]
WHERE"""
            if group_id == 'self':
                sqlText += " [UserID] = :UserID AND [GroupID] = :GroupID"
            else:
                sqlText += " [GroupID] = :GroupID"
            sqlText += """
    AND [CreateDate] >= (SELECT DATEADD(minute, -10, GETDATE()))
ORDER BY [CreateDate] ASC
"""
            select_sql = sqlalchemy.text(sqlText)
            results = conn.execute(select_sql, {"UserID": user_id, "GroupID": group_id}).fetchall()
        for result in results:
            if result[0].startswith('$UR:'):
                messages.append({"role": "user", "content": result[0][4:]})
            elif result[0].startswith('$AI:'):
                messages.append({"role": "assistant", "content": result[0][4:]})
        if len(messages) == 0: user_message = "請你用繁體中文回答。" + user_message
        messages.append({"role": "user", "content": user_message})
        response = ChatGPT(messages)
        if len(response) > 1024: response = response[:1024]
    
        LineBotApi.reply_message(
            event.reply_token,
            TextSendMessage(text=response)
        )

    elif (event.message.text.startswith('#IMG')):
        user_message = event.message.text[4:]
        img_url = ChatGPTimageB64(user_message)
        response = img_url

        LineBotApi.reply_message(
            event.reply_token,
            ImageSendMessage(original_content_url=img_url, preview_image_url=img_url)
        )

    if len(user_message) > 0:
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        user_sql_params = {"UserID": user_id, "GroupID": group_id, "Message": user_message, "AiFlag": 0, "CreateDate": now}
        ai_sql_params = {"UserID": user_id, "GroupID": group_id, "Message": response, "AiFlag": 1, "CreateDate": now}

        engine = connect_with_connector()
        insert_user_sql = sqlalchemy.text("INSERT INTO LineGroupHistoryMessage (UserID, GroupID, Message, AiFlag, CreateDate) VALUES (:UserID, :GroupID, :Message, :AiFlag, :CreateDate)")
        insert_ai_sql = sqlalchemy.text("INSERT INTO LineGroupHistoryMessage (UserID, GroupID, Message, AiFlag, CreateDate) VALUES (:UserID, :GroupID, :Message, :AiFlag, :CreateDate)")

        with engine.connect() as conn:
            result = conn.execute(insert_user_sql, user_sql_params)
            result = conn.execute(insert_ai_sql, ai_sql_params)
            conn.commit()
        engine.dispose()
    return "Ok"

def ChatGPT(inputMessage):
    res = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        max_tokens=1024,
        temperature=0.5,
        messages=inputMessage
    )
    completed_text = res.choices[0].message.content
    return completed_text

def SaveCloudImg(fileName, b64_json):
    storage_client = storage.Client()
    bucket_name = 'line-group-item'
    blob_name = 'images/'
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name + fileName)
    img_bytes = base64.b64decode(b64_json)
    blob.upload_from_string(img_bytes, content_type='image/png')
    storage_client.close()
    return f'https://storage.googleapis.com/{bucket_name}/{blob_name + fileName}'

def ChatGPTimageB64(inputMessage):
    res = openai.Image.create(
        prompt=inputMessage,
        n=1,
        size="256x256",
        response_format="b64_json"
    )
    url = SaveCloudImg(str(res['created']) + '.png', res['data'][0]['b64_json'])
    return url

def connect_with_connector() -> sqlalchemy.engine.base.Engine:
    def getconn() -> pytds.Connection:
        with Connector() as connector:
            conn = connector.connect(
                "testgpt-383706:us-central1:line-bot-database",
                "pytds",
                user="pyuser",
                password="jerry123",
                db="line-bot-db",
                ip_type=IPTypes.PUBLIC,
            )
            return conn
    engine = sqlalchemy.create_engine("mssql+pytds://", creator=getconn, echo=True)
    return engine

if __name__ == "__main__":
    app.run(host='0.0.0.0')