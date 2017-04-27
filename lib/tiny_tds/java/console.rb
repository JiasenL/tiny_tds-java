require 'pry'
module TinyTds
  module Java
    class Console
      def initialize
        Java.pry
      end
    end
  end
end