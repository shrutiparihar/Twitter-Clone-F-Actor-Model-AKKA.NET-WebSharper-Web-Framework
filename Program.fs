open System
open Akka.FSharp
open FSharp.Json
open Akka.Actor
open Suave
open Suave.Http
open Suave.Operators
open Suave.Filters
open Suave.Successful
open Suave.Files
open Suave.RequestErrors
open Suave.Logging
open Suave.Utils
open System.IO
open System.Net
open Suave.Sockets
open Suave.Sockets.Control
open Suave.WebSocket
open Newtonsoft.Json
open System.Threading
open Suave.RequestErrors
open Newtonsoft.Json.Serialization

//Create a system------->
let system = ActorSystem.Create("TwitterServer")

//The map that stores all the hashtags--------->
let mutable hashTagsMap = Map.empty
//The map that stores all the mentions--------->
let mutable mentionsMap = Map.empty
//The map that stores all the online users--------->
let mutable registeredUsers = Map.empty
//The map that stores all the followers--------->
let mutable globalfollowers = Map.empty

//setting number of users in current network-------->
let initialCapacity = 101
let numProcs = Environment.ProcessorCount

//defining the response class type------->
type ResponseType = {
  userID: string
  message: string
  service: string
  code: string
}

//defining the request class type------->
type RequestType = {
  userID: string
  value: string
}

//defining the type of messages in feed------->
type Showfeed = 
  | RegisterNewUser of (string)
  | Subscribers of (string*string)
  | AddActiveUsers of (string*WebSocket)
  | RemoveActiveUsers of (string)
  | UpdateFeeds of (string*string*string)

//defining the type of messages while tweeting------->
type TweetMessages = 
  | InitTweet of (IActorRef)
  | AddRegisteredUser of (string)
  | TweetRequest of (string*string*string)

//defining resource type class------>
type RestResource<'a> = {
    Entry : RequestType -> string
}

//defining an actor that reads the messgaes from the queue------->
let agent = MailboxProcessor<string*WebSocket>.Start(fun inbox ->
  let rec messageLoop() = async {
    //loop through messages from the websocket------->
    let! msg,webSkt = inbox.Receive()
    let byteRes =
      msg
      //Encode the message------->
      |> System.Text.Encoding.ASCII.GetBytes
      |> ByteSegment
    let! _ = webSkt.send Text byteRes true
    return! messageLoop()
  }
  messageLoop()
)

//This actor loops through feed messages-------->
let FeedActor (mailbox:Actor<_>) = 
  //initialize the following maps for each user-------->
  //followers of a user----->
  let mutable followers = Map.empty
  let mutable activeUsers = Map.empty
  let mutable feedtable = Map.empty
  let rec loop () = actor {
      //loop through all the messgaes in the message queue from feed------->
      let! message = mailbox.Receive() 
      match message with
      //if the message is registerUser type of messgae------->
      //Register a new user------->
      | RegisterNewUser(userId) ->
        followers <- Map.add userId Set.empty followers
        feedtable <- Map.add userId List.empty feedtable

      //if the message is Subscribe type of messgae------->
      //Subscribe a given user------->
      | Subscribers(userId, followerId) ->
        if followers.ContainsKey followerId then
          //if there exist ant user by that userID------->
          //follow it------->
          let mutable followSet = Set.empty
          followSet <- followers.[followerId]
          //Add the follower user to the follower list of the user that is followed------->
          followSet <- Set.add userId followSet
          followers <- Map.remove followerId followers 
          followers <- Map.add followerId followSet followers
          let mutable jsonData: ResponseType = 
            {userID = followerId; service= "Follow"; code = "OK"; message = sprintf "User %s started following you!" userId}
          let mutable consJson = Json.serialize jsonData
          agent.Post (consJson,activeUsers.[followerId])

      //if the message is Add Active user type of messgae------->
      //Add a newly active user to the active users list------->
      | AddActiveUsers(userId,userWebSkt) ->
        //the websocket sends the userID while sending the message------->
        if activeUsers.ContainsKey userId then  
          activeUsers <- Map.remove userId activeUsers
        //Add the active users along with the websocket------>
        activeUsers <- Map.add userId userWebSkt activeUsers 
        let mutable feedsPub = ""
        let mutable sertype = ""

        if feedtable.ContainsKey userId then
          let mutable feedsTop = ""
          let mutable fSize = 10
          let feedList:List<string> = feedtable.[userId]
          //if the active user doesn't have any activity------->
          //Show that there are no feed yet------->
          if feedList.Length = 0 then
            sertype <- "Follow"
            feedsPub <- sprintf "No feeds yet!!"
          //Or else------->
          //If there is any kind of activity on his account------->
          else
            //Show his top 10 newest feeds on his feed wall------->
            if feedList.Length < 10 then
                fSize <- feedList.Length
            for i in [0..(fSize-1)] do
              feedsTop <- "-" + feedtable.[userId].[i] + feedsTop

            feedsPub <- feedsTop
            sertype <- "LiveFeed"
          //Send a response to the websocket after adding the user-------->
          let jsonData: ResponseType = {userID = userId; message = feedsPub; code = "OK"; service=sertype}
          let consJson = Json.serialize jsonData
          agent.Post (consJson,userWebSkt) 
      
      //if the message is Remove Active user type of messgae------->
      //Remove an active user from the active users list------->
      | RemoveActiveUsers(userId) ->
        //the websocket sends the userID while sending the message------->
        //If there exist the userID sent------->
        if activeUsers.ContainsKey userId then  
          //remove the user------->
          activeUsers <- Map.remove userId activeUsers

      //if the message is Update user's feed type of messgae------->
      //Update an active user's feed with the activity happening------->
      | UpdateFeeds(userId,tweetMsg,sertype) ->
        //the websocket sends the userID while sending the message------->
        //If there exist the userID sent------->
        if followers.ContainsKey userId then
          let mutable stype = ""
          //if there is a tweet------->
          if sertype = "Tweet" then
            //update wall with following message------->
            stype <- sprintf "%s tweeted:" userId
          //if there is a re-tweet------->
          else 
            stype <- sprintf "%s re-tweeted:" userId
          //loop through all the followers and update their feeds------->
          for foll in followers.[userId] do 
            if followers.ContainsKey foll then
              //check if the follower is active------>
              if activeUsers.ContainsKey foll then
                //send the tweet message on his wall------>
                let twt = sprintf "%s^%s" stype tweetMsg
                //update the websocket------->
                let jsonData: ResponseType = {userID = foll; service=sertype; code="OK"; message = twt}
                let consJson = Json.serialize jsonData
                agent.Post (consJson,activeUsers.[foll])
              let mutable listy = []
              if feedtable.ContainsKey foll then
                  listy <- feedtable.[foll]
              listy  <- (sprintf "%s^%s" stype tweetMsg) :: listy
              feedtable <- Map.remove foll feedtable
              feedtable <- Map.add foll listy feedtable
      return! loop()
  }
  loop()

//The feed actor that does the job requested-------->
let feedActor = spawn system (sprintf "FeedActor") FeedActor

//a function that sends feed data to and from the websocket-------->
let liveFeed (webSocket : WebSocket) (context: HttpContext) =
  let rec loop() =
    let mutable presentUser = ""
    socket { 
      //read the message from the websocket------->
      let! msg = webSocket.read()
      match msg with
      //If there is a logout action------>
      | (Close, _, _) ->
        printfn "Closed WEBSOCKET"
        //ask the feed actor to remove the user from active user list------>
        feedActor <! RemoveActiveUsers(presentUser)
        let emptyResponse = [||] |> ByteSegment
        do! webSocket.send Close emptyResponse true
      //if there is another action besides closing------>
      | (Text, data, true) ->
        let reqMsg = UTF8.toString data
        //identify the request------->
        let parsed = Json.deserialize<RequestType> reqMsg
        presentUser <- parsed.userID
        //ask the feed actor to work on it------->
        feedActor <! AddActiveUsers(parsed.userID, webSocket)
        return! loop()
      | _ -> return! loop()
    }
  loop()

//The function that is called to register a new user-------->
let regNewUser userInput =
  let mutable resp = ""
  //Check is the userID is already registered-------->
  if registeredUsers.ContainsKey userInput.userID then
    //send an error message------->
    let rectype: ResponseType = {userID = userInput.userID; message = sprintf "User %s already registred" userInput.userID; service = "Register"; code = "FAIL"}
    resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  //if the userID is NOT already registered-------->
  else
    //add the new user to the registerUser Map------->
    registeredUsers <- Map.add userInput.userID userInput.value registeredUsers
    //empty its follower list------->
    globalfollowers <- Map.add userInput.userID Set.empty globalfollowers
    feedActor <! RegisterNewUser(userInput.userID)
    //Send the message to the websocket that user has been registered successfully-------->
    let rectype: ResponseType = {userID = userInput.userID; message = sprintf "User %s registered successfully" userInput.userID; service = "Register"; code = "OK"}
    resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  resp

//The function that is called to login a user-------->
let loginUser userInput =
  let mutable resp = ""
  //Check is the userID is registered or not-------->
  if registeredUsers.ContainsKey userInput.userID then
    //check if the passwords match------->
    if registeredUsers.[userInput.userID] = userInput.value then
      //allow login------->
      let rectype: ResponseType = {userID = userInput.userID; message = sprintf "User %s logged in successfully" userInput.userID; service = "Login"; code = "OK"}
      resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
    else 
      //show error message after authentication fail------->
      let rectype: ResponseType = {userID = userInput.userID; message = "Invalid userID / password"; service = "Login"; code = "FAIL"}
      resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  //send an error message------->
  else
    //tell the websokcet that the user requested doesn't exist------->
    let rectype: ResponseType = {userID = userInput.userID; message = "Invalid userID / password"; service = "Login"; code = "FAIL"}
    resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  resp

//The function that is called to follow a userID by a user-------->
let followUser userInput =
  let mutable resp = ""
  if userInput.value <> userInput.userID then
    //check if there exist any userID given by the user-------->
    if globalfollowers.ContainsKey userInput.value then
      //also check if the user is not already following it-------->
      if not (globalfollowers.[userInput.value].Contains userInput.userID) then
        let mutable tempset = globalfollowers.[userInput.value]
        tempset <- Set.add userInput.userID tempset
        //add the follower user to the follower list of the user being followed------->
        globalfollowers <- Map.remove userInput.value globalfollowers
        globalfollowers <- Map.add userInput.value tempset globalfollowers
        feedActor <! Subscribers(userInput.userID,userInput.value) 
        //send response to the websocket that user has been followed------->
        let rectype: ResponseType = {userID = userInput.userID; service="Follow"; message = sprintf "You started following %s!" userInput.value; code = "OK"}
        resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
      else 
        //Error message that the user is already following the userID provided-------->
        let rectype: ResponseType = {userID = userInput.userID; service="Follow"; message = sprintf "You are already following %s!" userInput.value; code = "FAIL"}
        resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString      
    else  
      //Error message that the userID provided does not exist-------->
      let rectype: ResponseType = {userID = userInput.userID; service="Follow"; message = sprintf "Invalid request, No such user (%s)." userInput.value; code = "FAIL"}
      resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  else
    //Error that the user tried following itself-------->
    let rectype: ResponseType = {userID = userInput.userID; service="Follow"; message = sprintf "You cannot follow yourself."; code = "FAIL"}
    resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString    
  // printfn "follow response: %s" resp
  resp

//The function that is called to create a tweet by a user-------->
let tweetUser userInput =
  let mutable resp = ""
  //breakdown a tweet------->
  if registeredUsers.ContainsKey userInput.userID then
    let mutable hashTag = ""
    let mutable mentionedUser = ""
    //Split the tweet word by word------->
    let parsed = userInput.value.Split ' '
    for parse in parsed do
      //if there exist any 'hashtag' or 'mentions'------->
      if parse.Length > 0 then
        if parse.[0] = '#' then
          hashTag <- parse.[1..(parse.Length-1)]
        else if parse.[0] = '@' then
          mentionedUser <- parse.[1..(parse.Length-1)]
    
    //if there is any mention------->
    if mentionedUser <> "" then
      //If theere exists any mentioned user in the network------->
      if registeredUsers.ContainsKey mentionedUser then
        if not (mentionsMap.ContainsKey mentionedUser) then
            //Add the mentioned user in the Mention Map--------->
            mentionsMap <- Map.add mentionedUser List.empty mentionsMap
        let mutable mList = mentionsMap.[mentionedUser]
        //update the feed wall of the mentioned user------->
        mList <- (sprintf "%s tweeted:^%s" userInput.userID userInput.value) :: mList
        mentionsMap <- Map.remove mentionedUser mentionsMap
        mentionsMap <- Map.add mentionedUser mList mentionsMap
        //Ask the feed actor to update the feed of the related users------->
        feedActor <! UpdateFeeds(userInput.userID,userInput.value,"Tweet")
        //Send the response to the websocket------->
        let rectype: ResponseType = {userID = userInput.userID; service="Tweet"; message = (sprintf "%s tweeted:^%s" userInput.userID userInput.value); code = "OK"}
        resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
      else
        //Display an error message that there is no user with 'MentionedUserID'--------->
        let rectype: ResponseType = {userID = userInput.userID; service="Tweet"; message = sprintf "Invalid request, mentioned user (%s) is not registered" mentionedUser; code = "FAIL"}
        resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
    else
      //If there are no mentions------>
      //Ask the feed actor to update the feed of the related users------->
      feedActor <! UpdateFeeds(userInput.userID,userInput.value,"Tweet")
      //Send the response to the websocket------->
      let rectype: ResponseType = {userID = userInput.userID; service="Tweet"; message = (sprintf "%s tweeted:^%s" userInput.userID userInput.value); code = "OK"}
      resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString

    //if there is any hashtag------->
    if hashTag <> "" then
      //If it is an new hashtag, add it to the hashtags list------->
      if not (hashTagsMap.ContainsKey hashTag) then
        hashTagsMap <- Map.add hashTag List.empty hashTagsMap
      //Print the tweet with the hashtag--------->
      let mutable tList = hashTagsMap.[hashTag]
      tList <- (sprintf "%s tweeted:^%s" userInput.userID userInput.value) :: tList
      hashTagsMap <- Map.remove hashTag hashTagsMap
      hashTagsMap <- Map.add hashTag tList hashTagsMap
  else  
    //Send an error message that the user does not exist-------->
    let rectype: ResponseType = {userID = userInput.userID; service="Tweet"; message = sprintf "Invalid request by user %s, Not registered yet!" userInput.userID; code = "FAIL"}
    resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  resp

//The function that is called to create a re-tweet by a user-------->
let retweetUser userInput =
  let mutable resp = ""
  //If the original user exists in the registered user's map------->
  if registeredUsers.ContainsKey userInput.userID then
    //Ask the feed actor to create the re-tweet------->
    feedActor <! UpdateFeeds(userInput.userID,userInput.value,"ReTweet")
    //Send the response to the websocket------->
    let rectype: ResponseType = {userID = userInput.userID; service="ReTweet"; message = (sprintf "%s re-tweeted:^%s" userInput.userID userInput.value); code = "OK"}
    resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  else  
    //Send an error message that the user does not exist-------->
    let rectype: ResponseType = {userID = userInput.userID; service="ReTweet"; message = sprintf "Invalid request by user %s, Not registered yet!" userInput.userID; code = "FAIL"}
    resp <- rectype |> Json.toJson |> System.Text.Encoding.UTF8.GetString
  resp

//The function that is called to create a query-------->
let query (userInput:string) = 
  let mutable tagsstring = ""
  let mutable mentionsString = ""
  let mutable resp = ""
  let mutable size = 10
  //check if the user has made any query or not-------->
  if userInput.Length > 0 then
    //if the query is a mention-------->
    if userInput.[0] = '@' then
      //search the user in the mentions Map--------->
      let searchKey = userInput.[1..(userInput.Length-1)]
      if mentionsMap.ContainsKey searchKey then
        //If the user exist in the mention Map------->
        let mapData:List<string> = mentionsMap.[searchKey]
        if (mapData.Length < 10) then
          size <- mapData.Length
        for i in [0..(size-1)] do
          //loop through all the mentions and display them------->
          mentionsString <- mentionsString + "-" + mapData.[i]
        let rectype: ResponseType = {userID = ""; service="Query"; message = mentionsString; code = "OK"}
        resp <- Json.serialize rectype
      else 
        //If there are no mentions of the user-------->
        //Display an  error message------->
        let rectype: ResponseType = {userID = ""; service="Query"; message = "-No tweets found for the mentioned user"; code = "OK"}
        resp <- Json.serialize rectype
    else
      //if not a mention query------>
      //Then a hashtag query------->
      let searchKey = userInput
      //If a hashtag already exist in the hashtag Map------->
      if hashTagsMap.ContainsKey searchKey then
        //obtain all the stored data from the hashtag Map------->
        //For a given hashtag-------->
        let mapData:List<string> = hashTagsMap.[searchKey]
        if (mapData.Length < 10) then
            size <- mapData.Length
        for i in [0..(size-1)] do
            //loop through all the hashtags and display them------->
            tagsstring <- tagsstring + "-" + mapData.[i]
        //Send the websocket message------->
        let rectype: ResponseType = {userID = ""; service="Query"; message = tagsstring; code = "OK"}
        resp <- Json.serialize rectype
      else 
        //If there are no hashtag found-------->
        //Display an  error message------->
        let rectype: ResponseType = {userID = ""; service="Query"; message = "-No tweets found for the hashtag"; code = "OK"}
        resp <- Json.serialize rectype
  else
    //If there is no input query found-------->
    //Display an  error message------->
    let rectype: ResponseType = {userID = ""; service="Query"; message = "Type something to search"; code = "FAIL"}
    resp <- Json.serialize rectype
  resp


let respInJson v =     
    let jsonSerializerSettings = JsonSerializerSettings()
    jsonSerializerSettings.ContractResolver <- CamelCasePropertyNamesContractResolver()
    JsonConvert.SerializeObject(v, jsonSerializerSettings)
    |> OK 
    >=> Writers.setMimeType "application/json; charset=utf-8"

let respJson (v:string) =     
    let jsonSerializerSettings = JsonSerializerSettings()
    jsonSerializerSettings.ContractResolver <- CamelCasePropertyNamesContractResolver()
    JsonConvert.SerializeObject(v, jsonSerializerSettings)
    |> OK 
    >=> Writers.setMimeType "application/json; charset=utf-8"

let fromJson<'a> json =
        JsonConvert.DeserializeObject(json, typeof<'a>) :?> 'a    

let getReqResource<'a> (requestInp : HttpRequest) = 
    let getInString (rawForm:byte[]) = System.Text.Encoding.UTF8.GetString(rawForm)
    let requestArr:byte[] = requestInp.rawForm
    requestArr |> getInString |> fromJson<RequestType>

let entryRequest resourceName resource = 
  let resourcePath = "/" + resourceName

  let entryDone userInput =
    let userRegResp = resource.Entry userInput
    userRegResp

  choose [
    path resourcePath >=> choose [
      POST >=> request (getReqResource >> entryDone >> respInJson)
    ]
  ]

//A single API for each type of the server------>
//The request is sent to each of these endpoints for different types of services------>
let RegisterNewUserPoint = entryRequest "register" {
  Entry = regNewUser
}

let LoginUserPoint = entryRequest "login" {
  Entry = loginUser
}

let FollowUserPoint = entryRequest "follow" {
  Entry = followUser
}

let TweetUserPoint = entryRequest "tweet" {
  Entry = tweetUser
}

let ReTweetUserPoint = entryRequest "retweet" {
  Entry = retweetUser
}

let ws = 
  choose [
    //A WebSocket Endpoint------->
    path "/livefeed" >=> handShake liveFeed
    RegisterNewUserPoint
    LoginUserPoint
    FollowUserPoint
    TweetUserPoint
    ReTweetUserPoint
    pathScan "/search/%s"
      (fun searchkey ->
        let keyval = (sprintf "%s" searchkey)
        let reply = query keyval
        OK reply) 
    GET >=> choose [path "/" >=> file "Login.html"; browseHome]
  ]

[<EntryPoint>]
let main _ =
  startWebServer { defaultConfig with logger = Targets.create Verbose [||] } ws
  0
