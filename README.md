# TinyTds::Java

This gem is only ment to fullfil gem dependencies for activerecord-sqlserver-adapter on jruby.
It does not contain any relevant code and only gives hints for jruby users how to use the sequel gem as a connection adapter.

## Installation

Add this line to your application's Gemfile:

```ruby
platform :jruby do
  gem 'tiny_tds'
  gem 'sequel'
  gem 'jdbc-mssql-azure'
end
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install tiny_tds

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/iaddict/tiny_tds-java.

