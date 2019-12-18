abstract class Notification

case class Email(sender: String, title: String, body: String) extends Notification
case class SMS(caller: String, message: String) extends Notification

object a{
        def main(args:Array[String]){
                def showNotification(notification: Notification): String = {
                        notification match {
                          case Email(email, title, _) =>
                            s"You got an email from $email with title: $title"
                          case SMS(number, message) =>
                            s"You got an SMS from $number! Message: $message"
                        }
                }
            val sSMS=SMS("12345","are u der?")
            val sEmail=Email("xyz@email.com","BD","BD class")
            
            println(showNotification(sSMS))
            println(showNotification(sEmail))
        }
}
