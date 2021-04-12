using System;

namespace Aletheia.Engine.Cloud.User
{
    public enum ApiCallDirection
    {
        Request = 0, //A client requested data from our servers (i.e. they made a get call)
        Push = 1 //We pushed data to one of their endpoints (in the case of a webhook)
    }
}